package stream

// Subscription stores the server view of a given type subscription in a stream.
type Subscription struct {
	// wildcard indicates if the subscription currently has a wildcard watch.
	wildcard bool

	// subscribedResourceNames provides the resources explicitly requested by the client
	// This list might be non-empty even when set as wildcard.
	subscribedResourceNames map[string]struct{}

	// returnedResources contains the resources acknowledged by the client and the acknowledged versions.
	returnedResources map[string]string
}

// NewSubscription initializes a subscription state.
func NewSubscription(wildcard bool, initialResourceVersions map[string]string) Subscription {
	state := Subscription{
		wildcard:                wildcard,
		subscribedResourceNames: map[string]struct{}{},
		returnedResources:       initialResourceVersions,
	}

	if initialResourceVersions == nil {
		state.returnedResources = make(map[string]string)
	}

	return state
}

// SubscribedResources returns the list of resources currently explicitly subscribed to
// If the request is set to wildcard it may be empty
// Can only be used for delta watches
// TODO(valerian-roche): set those resources properly for sotw subscriptions
func (s Subscription) SubscribedResources() map[string]struct{} {
	return s.subscribedResourceNames
}

func (s *Subscription) SetSubscribedResources(resources map[string]struct{}) {
	if resources != nil {
		s.subscribedResourceNames = resources
	} else {
		s.subscribedResourceNames = make(map[string]struct{})
	}
}

// IsWildcard returns whether or not the subscription currently has a wildcard watch
// Can only be used for delta watches
// TODO(valerian-roche): set those resources properly for sotw subscriptions
func (s Subscription) IsWildcard() bool {
	return s.wildcard
}

// SetWildcard sets whether the subscription is using a wildcard watch, whether legacy or not
func (s *Subscription) SetWildcard(wildcard bool) {
	s.wildcard = wildcard
}

// WatchesResources returns whether at least one of the resources provided is currently being watched by the subscription.
// If the request is wildcard, it will always return true,
// otherwise it will compare the provided resources to the list of resources currently subscribed
// Can only be used for delta watches
func (s Subscription) WatchesResources(resourceNames map[string]struct{}) bool {
	if s.wildcard {
		return true
	}
	for resourceName := range resourceNames {
		if _, ok := s.subscribedResourceNames[resourceName]; ok {
			return true
		}
	}
	return false
}

// ReturnedResources returns the list of resources returned to the client
// and their version
func (s Subscription) ReturnedResources() map[string]string {
	return s.returnedResources
}

// SetReturnedResources sets a list of resource versions currently known by the client
// The cache can use this state to compute resources added/updated/deleted
func (s *Subscription) SetReturnedResources(resourceVersions map[string]string) {
	s.returnedResources = resourceVersions
}
