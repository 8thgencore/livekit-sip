package sip

import "testing"

import "github.com/stretchr/testify/require"

func TestNormalizeRepeatedSIPPort(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "empty", input: "", want: ""},
		{name: "plain host", input: "pbx.uiscom.ru", want: "pbx.uiscom.ru"},
		{name: "single port", input: "pbx.uiscom.ru:5060", want: "pbx.uiscom.ru:5060"},
		{name: "repeated port", input: "pbx.uiscom.ru:5060:5060", want: "pbx.uiscom.ru:5060"},
		{name: "triple repeated port", input: "pbx.uiscom.ru:5060:5060:5060", want: "pbx.uiscom.ru:5060"},
		{name: "different ports stay unchanged", input: "pbx.uiscom.ru:5060:5070", want: "pbx.uiscom.ru:5060:5070"},
		{name: "ipv6 left unchanged", input: "[2001:db8::1]:5060", want: "[2001:db8::1]:5060"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, normalizeRepeatedSIPPort(tt.input))
		})
	}
}

func TestCreateURIFromUserAndAddressNormalizesRepeatedPort(t *testing.T) {
	uri := CreateURIFromUserAndAddress("user", "pbx.uiscom.ru:5060:5060", TransportUDP)

	require.Equal(t, "pbx.uiscom.ru", uri.Host)
	require.Equal(t, 5060, uri.GetPort())
	require.Equal(t, "pbx.uiscom.ru:5060", uri.GetDest())
}

func TestNormalizeSIPHostname(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "plain host", input: "pbx.uiscom.ru", want: "pbx.uiscom.ru"},
		{name: "host with port", input: "pbx.uiscom.ru:5060", want: "pbx.uiscom.ru"},
		{name: "host with repeated port", input: "pbx.uiscom.ru:5060:5060", want: "pbx.uiscom.ru"},
		{name: "different ports stay unchanged", input: "pbx.uiscom.ru:5060:5070", want: "pbx.uiscom.ru:5060:5070"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, normalizeSIPHostname(tt.input))
		})
	}
}
