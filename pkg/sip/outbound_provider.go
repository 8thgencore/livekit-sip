package sip

import (
	"net"
	"strings"
)

type outboundProviderProfile struct {
	ID                                  string
	SkipRegistrationInAuto              bool
	AllowRegisteredInviteDirectFallback bool
	DirectAuthFailureIsConfigError      bool
	DeleteTrunkAfterCall                bool
	DefaultG711Only                     bool
	OutboundQueueScope                  outboundProviderQueueScope
	OutboundMaxConcurrentCalls          int
}

type outboundProviderQueueScope string

const (
	outboundProviderQueueScopeTrunk        outboundProviderQueueScope = "trunk"
	outboundProviderQueueScopeProviderFrom outboundProviderQueueScope = "provider_from"
)

var universalOutboundProviderProfile = outboundProviderProfile{
	ID:                                  "universal",
	AllowRegisteredInviteDirectFallback: true,
	OutboundQueueScope:                  outboundProviderQueueScopeTrunk,
	OutboundMaxConcurrentCalls:          outboundPerTrunkMaxConcurrentCalls,
}

var outboundProviderDomainProfiles = map[string]outboundProviderProfile{
	"novofon.ru": {
		ID:                             "novofon",
		SkipRegistrationInAuto:         true,
		DirectAuthFailureIsConfigError: true,
	},
	"uiscom.ru": {
		ID:                                  "uiscom",
		AllowRegisteredInviteDirectFallback: true,
		DefaultG711Only:                     true,
	},
	"plusofon.ru": {
		ID:                                  "plusofon",
		AllowRegisteredInviteDirectFallback: true,
		DefaultG711Only:                     true,
	},
	"megapbx.ru": {
		ID:                                  "megapbx",
		AllowRegisteredInviteDirectFallback: true,
		DefaultG711Only:                     true,
	},
	"mtt.ru": {
		ID:                                  "mtt",
		AllowRegisteredInviteDirectFallback: true,
	},
	"mangosip.ru": {
		ID:                                  "mangosip",
		AllowRegisteredInviteDirectFallback: true,
	},
	"sipuni.ru": {
		ID:                                  "sipuni",
		AllowRegisteredInviteDirectFallback: true,
		DeleteTrunkAfterCall:                true,
		OutboundQueueScope:                  outboundProviderQueueScopeProviderFrom,
		OutboundMaxConcurrentCalls:          1,
	},
}

func outboundProviderProfileForAddress(address string) outboundProviderProfile {
	host := normalizeSIPHost(address)
	for domain, profile := range outboundProviderDomainProfiles {
		if host == domain || strings.HasSuffix(host, "."+domain) {
			profile.applyDefaults()
			return profile
		}
	}
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
	return outboundProviderProfileForAddress(address).DeleteTrunkAfterCall
}

func normalizeSIPHost(address string) string {
	host := address
	if parsedHost, _, err := net.SplitHostPort(address); err == nil {
		host = parsedHost
	}
	return strings.ToLower(strings.TrimSuffix(host, "."))
}
