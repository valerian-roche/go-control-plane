package delta

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	"github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"github.com/envoyproxy/go-control-plane/pkg/server/stream/v3"
)

// Server is a wrapper interface which is meant to hold the proper stream handler for each xDS protocol.
type Server interface {
	DeltaStreamHandler(stream stream.DeltaStream, typeURL string) error
}

type Callbacks interface {
	// OnDeltaStreamOpen is called once an incremental xDS stream is open with a stream ID and the type URL (or "" for ADS).
	// Returning an error will end processing and close the stream. OnStreamClosed will still be called.
	OnDeltaStreamOpen(context.Context, int64, string) error
	// OnDeltaStreamClosed is called immediately prior to closing an xDS stream with a stream ID.
	OnDeltaStreamClosed(int64, *core.Node)
	// OnStreamDeltaRequest is called once a request is received on a stream.
	// Returning an error will end processing and close the stream. OnStreamClosed will still be called.
	OnStreamDeltaRequest(int64, *discovery.DeltaDiscoveryRequest) error
	// OnStreamDelatResponse is called immediately prior to sending a response on a stream.
	OnStreamDeltaResponse(int64, *discovery.DeltaDiscoveryRequest, *discovery.DeltaDiscoveryResponse)
}

type ExtendedCallbacks interface {
	OnStreamDeltaResponseF(int64, *discovery.DeltaDiscoveryRequest, *discovery.DeltaDiscoveryResponse, func(typeURL string, resourceNames []string))
}

var deltaErrorResponse = &cache.RawDeltaResponse{}

type server struct {
	cache     cache.ConfigWatcher
	callbacks Callbacks

	// total stream count for counting bi-di streams
	streamCount int64
	ctx         context.Context
}

// NewServer creates a delta xDS specific server which utilizes a ConfigWatcher and delta Callbacks.
func NewServer(ctx context.Context, config cache.ConfigWatcher, callbacks Callbacks) Server {
	return &server{
		cache:     config,
		callbacks: callbacks,
		ctx:       ctx,
	}
}

func (s *server) processDelta(str stream.DeltaStream, reqCh <-chan *discovery.DeltaDiscoveryRequest, defaultTypeURL string) error {
	streamID := atomic.AddInt64(&s.streamCount, 1)

	// streamNonce holds a unique nonce for req-resp pairs per xDS stream.
	var streamNonce int64

	// a collection of stack allocated watches per request type
	watches := newWatches()

	var node = &core.Node{}

	defer func() {
		watches.Cancel()
		if s.callbacks != nil {
			s.callbacks.OnDeltaStreamClosed(streamID, node)
		}
	}()

	// Sets up a watch in the cache
	processWatch := func(newWatch watch, watchTypeURL string) {
		if newWatch.req == nil {
			// This watch does not include an actual client watch
			// Only store the state
			watches.deltaWatches[watchTypeURL] = newWatch
			return
		}
		newWatch.responses = make(chan cache.DeltaResponse, 1)
		newWatch.cancel = s.cache.CreateDeltaWatch(newWatch.req, newWatch.state, newWatch.responses)
		watches.deltaWatches[watchTypeURL] = newWatch

		go func() {
			resp, more := <-newWatch.responses
			if more {
				watches.deltaMuxedResponses <- resp
			}
		}()
	}

	// Sends a response, returns the new stream nonce
	send := func(resp cache.DeltaResponse) (string, error) {
		if resp == nil {
			return "", errors.New("missing response")
		}

		response, err := resp.GetDeltaDiscoveryResponse()
		if err != nil {
			return "", err
		}

		streamNonce = streamNonce + 1
		response.Nonce = strconv.FormatInt(streamNonce, 10)
		if s.callbacks != nil {
			s.callbacks.OnStreamDeltaResponse(streamID, resp.GetDeltaRequest(), response)

			if extendedCallbacks, ok := s.callbacks.(ExtendedCallbacks); ok {
				extendedCallbacks.OnStreamDeltaResponseF(streamID, resp.GetDeltaRequest(), response, func(typeURL string, resourceNames []string) {
					// Check if there is an existing state for this URL type
					// If yes and there is a watch on it (i.e. req is set),
					//   - cancel the watch (as state is immutable while a watch is running)
					//   - reset the version for the resources we want to trigger
					//   - restart a watch
					// If there is a state with no watch
					//   - simply update the version for mentioned resources to be tracked
					// If there is no watch
					//   - create an empty one with the versions we want
					watch, ok := watches.deltaWatches[typeURL]
					if !ok {
						// There's no existing watch
						// Create a state to track resources we want to invalidate
						// This watch will not have a request and won't be cancelled/watched until a proper request is received
						watch.state = stream.NewStreamState(false, nil)
					} else {
						watch.Cancel()
					}
					for _, resourceName := range resourceNames {
						// This will force an update for those resources
						// If not existing it will return them as removed
						// /!\ If not using wildcard, only watched resources will be returned (set or removed)
						watch.state.GetResourceVersions()[resourceName] = ""
					}

					processWatch(watch, typeURL)
				})
			}
		}

		return response.Nonce, str.Send(response)
	}

	if s.callbacks != nil {
		if err := s.callbacks.OnDeltaStreamOpen(str.Context(), streamID, defaultTypeURL); err != nil {
			return err
		}
	}

	for {
		select {
		case <-s.ctx.Done():
			return nil
		case resp, more := <-watches.deltaMuxedResponses:
			if !more {
				break
			}

			typ := resp.GetDeltaRequest().GetTypeUrl()
			if resp == deltaErrorResponse {
				return status.Errorf(codes.Unavailable, typ+" watch failed")
			}

			nonce, err := send(resp)
			if err != nil {
				return err
			}

			watch := watches.deltaWatches[typ]
			watch.nonce = nonce

			watch.state.SetResourceVersions(resp.GetNextVersionMap())
			watches.deltaWatches[typ] = watch
		case req, more := <-reqCh:
			// input stream ended or errored out
			if !more {
				return nil
			}
			if req == nil {
				return status.Errorf(codes.Unavailable, "empty request")
			}

			if s.callbacks != nil {
				if err := s.callbacks.OnStreamDeltaRequest(streamID, req); err != nil {
					return err
				}
			}

			// The node information might only be set on the first incoming delta discovery request, so store it here so we can
			// reset it on subsequent requests that omit it.
			if req.Node != nil {
				node = req.Node
			} else {
				req.Node = node
			}

			// type URL is required for ADS but is implicit for any other xDS stream
			if defaultTypeURL == resource.AnyType {
				if req.TypeUrl == "" {
					return status.Errorf(codes.InvalidArgument, "type URL is required for ADS")
				}
			} else if req.TypeUrl == "" {
				req.TypeUrl = defaultTypeURL
			}

			typeURL := req.GetTypeUrl()

			// Create a watch for this request
			// Three possible states here:
			//  - there is no state for this type
			//     -> Create a new state, attach the request and start the watch
			//  - there is an existing state without a watch (i.e. no request attached)
			//     -> Update the state with initial resources provided (as well as setting legacy wildcard status)
			//        Consider versions defined on the state as taking precedence over the ones set by envoy
			//     This behavior is used to allow force-pushing resources to envoy.
			//     This is itself used to workaround envoy bugs in its delta state-machine.
			//  - there is an existing state with a watch
			//     -> cancel the watch (a state with an active watch is immutable), then update the state with the new request parameters
			watch, ok := watches.deltaWatches[typeURL]
			if !ok {
				// Initialize the state of the stream.
				// Since there was no previous state, we know we're handling the first request of this type
				// so we set the initial resource versions if we have any.
				// We also set the stream as wildcard based on its legacy meaning (no resource name sent in resource_names_subscribe).
				// If the state starts with this legacy mode, adding new resources will not unsubscribe from wildcard.
				// It can still be done by explicitly unsubscribing from "*"
				watch.state = stream.NewStreamState(len(req.GetResourceNamesSubscribe()) == 0, req.GetInitialResourceVersions())
			} else if watch.req == nil {
				// There is a state but no active watch
				// Update the state with the request information in the same way as above
				watch.state.SetWildcard(len(req.GetResourceNamesSubscribe()) == 0)
				versions := make(map[string]string, len(req.GetInitialResourceVersions()))
				for resource, version := range req.GetInitialResourceVersions() {
					// Versions defined on the state take precedence to versions provided by envoy
					// This allows force-pushing resources to envoy even if the versions have not changed
					if stateVersion, ok := watch.state.GetResourceVersions()[resource]; ok {
						version = stateVersion
					}
					versions[resource] = version
				}
				watch.state.SetResourceVersions(versions)
			} else {
				// There is an active watch, cancel it before altering anything
				watch.Cancel()
			}

			s.subscribe(req.GetResourceNamesSubscribe(), &watch.state)
			s.unsubscribe(req.GetResourceNamesUnsubscribe(), &watch.state)

			watch.req = req
			processWatch(watch, typeURL)
		}
	}
}

func (s *server) DeltaStreamHandler(str stream.DeltaStream, typeURL string) error {
	// a channel for receiving incoming delta requests
	reqCh := make(chan *discovery.DeltaDiscoveryRequest)

	// we need to concurrently handle incoming requests since we kick off processDelta as a return
	go func() {
		for {
			select {
			case <-str.Context().Done():
				close(reqCh)
				return
			default:
				req, err := str.Recv()
				if err != nil {
					close(reqCh)
					return
				}

				reqCh <- req
			}
		}
	}()

	return s.processDelta(str, reqCh, typeURL)
}

// When we subscribe, we just want to make the cache know we are subscribing to a resource.
// Even if the stream is wildcard, we keep the list of explicitly subscribed resources as the wildcard subscription can be discarded later on.
func (s *server) subscribe(resources []string, streamState *stream.StreamState) {
	sv := streamState.GetSubscribedResourceNames()
	for _, resource := range resources {
		if resource == "*" {
			streamState.SetWildcard(true)
			continue
		}
		sv[resource] = struct{}{}
	}
}

// Unsubscriptions remove resources from the stream's subscribed resource list.
// If a client explicitly unsubscribes from a wildcard request, the stream is updated and now watches only subscribed resources.
func (s *server) unsubscribe(resources []string, streamState *stream.StreamState) {
	sv := streamState.GetSubscribedResourceNames()
	for _, resource := range resources {
		if resource == "*" {
			streamState.SetWildcard(false)
			continue
		}
		if _, ok := sv[resource]; ok && streamState.IsWildcard() {
			// The XDS protocol states that:
			// * if a watch is currently wildcard
			// * a resource is explicitly unsubscribed by name
			// Then the control-plane must return in the response whether the resource is removed (if no longer present for this node)
			// or still existing. In the latter case the entire resource must be returned, same as if it had been created or updated
			// To achieve that, we mark the resource as having been returned with an empty version. While creating the response, the cache will either:
			// * detect the version change, and return the resource (as an update)
			// * detect the resource deletion, and set it as removed in the response
			streamState.GetResourceVersions()[resource] = ""
		}
		delete(sv, resource)
	}
}
