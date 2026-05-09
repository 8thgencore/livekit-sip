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
}

var universalOutboundProviderProfile = outboundProviderProfile{
	ID:                                  "universal",
	AllowRegisteredInviteDirectFallback: true,
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
		DeleteTrunkAfterCall:                true,
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
	},
}

func outboundProviderProfileForAddress(address string) outboundProviderProfile {
	host := normalizeSIPHost(address)
	for domain, profile := range outboundProviderDomainProfiles {
		if host == domain || strings.HasSuffix(host, "."+domain) {
			return profile
		}
	}
	return universalOutboundProviderProfile
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
