package sotw

import (
	"reflect"
	"sync/atomic"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	"github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"github.com/envoyproxy/go-control-plane/pkg/server/stream/v3"
)

// process handles a bi-di stream request
func (s *server) process(str stream.Stream, reqCh chan *discovery.DiscoveryRequest, defaultTypeURL string) error {
	// create our streamWrapper which can be passed down to sub control loops.
	// this is useful for abstracting critical information for various types of
	// xDS resource processing.
	sw := streamWrapper{
		stream:    str,
		ID:        atomic.AddInt64(&s.streamCount, 1), // increment stream count
		callbacks: s.callbacks,
		node:      &core.Node{}, // node may only be set on the first discovery request

		// a collection of stack allocated watches per request type.
		subscriptions: newSubscriptions(),
	}

	// cleanup once our stream has ended.
	defer sw.shutdown()

	if s.callbacks != nil {
		if err := s.callbacks.OnStreamOpen(str.Context(), sw.ID, defaultTypeURL); err != nil {
			return err
		}
	}

	// do an initial recompute so we can load the first 2 channels:
	// <-reqCh
	// s.ctx.Done()
	sw.subscriptions.recomputeWatches(s.ctx, reqCh)

	for {
		// The list of select cases looks like this:
		// 0: <- ctx.Done
		// 1: <- reqCh
		// 2...: per type watches
		index, value, ok := reflect.Select(sw.subscriptions.cases)
		switch index {
		// ctx.Done() -> if we receive a value here we return
		// as no further computation is needed
		case 0:
			return nil
		// Case 1 handles any request inbound on the stream
		// and handles all initialization as needed
		case 1:
			// input stream ended or failed
			if !ok {
				return nil
			}

			req := value.Interface().(*discovery.DiscoveryRequest)
			if req == nil {
				return status.Errorf(codes.Unavailable, "empty request")
			}

			// Only first request is guaranteed to hold node info so if it's missing, reassign.
			if req.Node != nil {
				sw.node = req.Node
			} else {
				req.Node = sw.node
			}

			// nonces can be reused across streams; we verify nonce only if nonce is not initialized
			nonce := req.GetResponseNonce()

			// type URL is required for ADS but is implicit for xDS
			if defaultTypeURL == resource.AnyType {
				if req.TypeUrl == "" {
					return status.Errorf(codes.InvalidArgument, "type URL is required for ADS")
				}

				// When using ADS we need to order responses.
				// This is guaranteed in the xDS protocol specification
				// as ADS is required to be eventually consistent.
				// More details can be found here if interested:
				// https://www.envoyproxy.io/docs/envoy/latest/api-docs/xds_protocol#eventual-consistency-considerations
				if s.opts.Ordered {
					// send our first request on the stream again so it doesn't get
					// lost in processing on the new control loop
					// There's a risk (albeit very limited) that we'd end up handling requests in the wrong order here.
					// If envoy is using ADS for endpoints, and clusters are added in short sequence,
					// the following request might include a new cluster and be discarded as the previous one will be handled after.
					go func() {
						reqCh <- req
					}()

					// Trigger a different code path specifically for ADS.
					// We want resource ordering so things don't get sent before they should.
					// This is a blocking call and will exit the process function
					// on successful completion.
					return s.processADS(&sw, reqCh, defaultTypeURL)
				}
			} else if req.TypeUrl == "" {
				req.TypeUrl = defaultTypeURL
			}

			if s.callbacks != nil {
				if err := s.callbacks.OnStreamRequest(sw.ID, req); err != nil {
					return err
				}
			}

			// sub must not be modified until any potential watch is closed
			// as we commit in the Cache interface that it is immutable for the lifetime of the watch
			sub, ok := sw.subscriptions.responders[req.GetTypeUrl()]
			if ok {
				// Existing subscription, lets check and update if needed.
				// If these requirements aren't satisfied, leave an open watch.
				if sub.nonce == "" || sub.nonce == nonce {
					sub.watch.close()
				} else {
					// The request does not match the previous nonce.
					// Currently we drop the new request in this context
					break
				}

				// Record Resource names that a client has received.
				sub.state.SetKnownResources(sub.lastResponseResources)
			} else {
				sub = newSubscription(len(req.ResourceNames) == 0)
			}

			updateSubscriptionResources(req, &sub.state)

			responder := make(chan cache.Response, 1)
			cancel, err := s.cache.CreateWatch(req, sub.state, responder)
			if err != nil {
				return err
			}
			sub.watch = watch{
				cancel:   cancel,
				response: responder,
			}

			sw.subscriptions.addSubscription(req.TypeUrl, sub)

			// Recompute the dynamic select cases for this stream.
			sw.subscriptions.recomputeWatches(s.ctx, reqCh)
		default:
			// Channel n -> these are the dynamic list of responders that correspond to the stream request typeURL
			if !ok {
				// Receiver channel was closed. TODO(jpeach): probably cancel the watch or something?
				return status.Errorf(codes.Unavailable, "resource watch %d -> failed", index)
			}

			res := value.Interface().(cache.Response)
			nonce, err := sw.send(res)
			if err != nil {
				return err
			}

			sw.subscriptions.responders[res.GetRequest().GetTypeUrl()].nonce = nonce
		}
	}
}

// updateSubscriptionResources provides a normalized view of resources to be used in Cache
// It is also implementing the new behavior of wildcard as described in
// https://www.envoyproxy.io/docs/envoy/latest/api-docs/xds_protocol#how-the-client-specifies-what-resources-to-return
func updateSubscriptionResources(req *discovery.DiscoveryRequest, subscriptionState *stream.SubscriptionState) {
	subscribedResources := make(map[string]struct{}, len(req.ResourceNames))
	explicitWildcard := false
	for _, resource := range req.ResourceNames {
		if resource == "*" {
			explicitWildcard = true
		} else {
			subscribedResources[resource] = struct{}{}
		}
	}

	if subscriptionState.IsWildcard() && len(req.ResourceNames) == 0 && len(subscriptionState.GetSubscribedResources()) == 0 {
		// We were wildcard and no resource has been subscribed
		// Legacy wildcard mode states that we remain in wildcard mode
		subscriptionState.SetWildcard(true)
	} else if explicitWildcard {
		// Explicit subscription to wildcard
		// Documentation states that we should no longer allow to fallback to the previous case
		// and no longer setting wildcard would no longer subscribe to anything
		// For now we ignore this case and will not support unsubscribing in this case
		subscriptionState.SetWildcard(true)
	} else {
		// The subscription is currently not wildcard, or there are resources or have been resources subscribed to
		// This is no longer the legacy wildcard case as described by the specification
		subscriptionState.SetWildcard(false)
	}
	subscriptionState.SetSubscribedResources(subscribedResources)

}
