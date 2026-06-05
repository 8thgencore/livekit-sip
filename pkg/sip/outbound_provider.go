package sip

import (
	"strings"
	"time"
)

type outboundProviderProfile struct {
	ID                                    string
	SkipRegistrationInAuto                bool
	AllowRegisteredInviteDirectFallback   bool
	DirectAuthFailureIsConfigError        bool
	DeleteTrunkAfterCall                  bool
	DefaultG711Only                       bool
	RouteRegisteredInviteToRegistrar      bool
	RouteRegistrationToRegistrar          bool
	ResolveRegistrationToIP               bool
	DisableRegistrationCache              bool
	AlwaysRefreshRegistrationBeforeInvite bool
	MaxRegistrationAgeBeforeInvite        time.Duration
	RegisterInviteSettlingDelay           time.Duration
	OutboundQueueScope                    outboundProviderQueueScope
	OutboundMaxConcurrentCalls            int
}

type outboundProviderQueueScope string

const (
	outboundProviderQueueScopeTrunk        outboundProviderQueueScope = "trunk"
	outboundProviderQueueScopeProviderFrom outboundProviderQueueScope = "provider_from"
)

var universalOutboundProviderProfile = outboundProviderProfile{
	ID:                                    "universal",
	AllowRegisteredInviteDirectFallback:   true,
	DefaultG711Only:                       true,
	RouteRegisteredInviteToRegistrar:      true,
	DisableRegistrationCache:              true,
	AlwaysRefreshRegistrationBeforeInvite: true,
	OutboundQueueScope:                    outboundProviderQueueScopeTrunk,
	OutboundMaxConcurrentCalls:            outboundPerTrunkMaxConcurrentCalls,
}

func outboundProviderProfileForAddress(address string) outboundProviderProfile {
	profile := universalOutboundProviderProfile
	profile.applyDefaults()
	return profile
}

func (p *outboundProviderProfile) applyDefaults() {
	if p.OutboundQueueScope == "" {
		p.OutboundQueueScope = outboundProviderQueueScopeTrunk
	}
	if p.OutboundMaxConcurrentCalls <= 0 {
		p.OutboundMaxConcurrentCalls = outboundPerTrunkMaxConcurrentCalls
	}
}

func ShouldDeleteOutboundTrunkAfterCall(address string) bool {
	return strings.TrimSpace(address) != ""
}
