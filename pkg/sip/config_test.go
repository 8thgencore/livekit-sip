package sip

import (
	"net/netip"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/livekit/sip/pkg/config"
)

func TestGetServiceConfigUsesListenIPAsLocalSignalingIP(t *testing.T) {
	conf := &config.Config{
		ListenIP: "10.10.10.5",
	}

	sconf, err := GetServiceConfig(conf)
	require.NoError(t, err)
	require.Equal(t, netip.MustParseAddr("10.10.10.5"), sconf.SignalingIPLocal)
	require.Equal(t, sconf.SignalingIPLocal, sconf.SignalingIP)
	require.Equal(t, sconf.SignalingIP, sconf.MediaIP)
}

func TestGetServiceConfigKeepsLocalBindIPWhenUsingNAT1To1(t *testing.T) {
	conf := &config.Config{
		ListenIP:       "10.10.10.5",
		NAT1To1IP:      "203.0.113.10",
		LocalNet:       "10.10.10.0/24",
		MediaNAT1To1IP: "198.51.100.20",
	}

	sconf, err := GetServiceConfig(conf)
	require.NoError(t, err)
	require.Equal(t, netip.MustParseAddr("10.10.10.5"), sconf.SignalingIPLocal)
	require.Equal(t, netip.MustParseAddr("203.0.113.10"), sconf.SignalingIP)
	require.Equal(t, netip.MustParseAddr("198.51.100.20"), sconf.MediaIP)
}

func TestGetSignalingLocalIPFallsBackWhenListenIPIsWildcard(t *testing.T) {
	conf := &config.Config{
		ListenIP: "0.0.0.0",
	}

	ip, err := getSignalingLocalIP(conf)
	require.NoError(t, err)
	require.True(t, ip.IsValid())
	require.False(t, ip.IsUnspecified())
}
