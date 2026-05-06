package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/sip/pkg/sip"
)

type trunkMetadata struct {
	SIPEndpoint *struct {
		Host      string `json:"host"`
		Port      int    `json:"port"`
		Transport string `json:"transport"`
	} `json:"sip_endpoint"`
	AuthUser string `json:"auth_user"`
}

func (s *Service) enrichRegisterFromInboundTrunkMetadata(ctx context.Context, info sip.AuthInfo) sip.AuthInfo {
	if s == nil || s.sipClient == nil {
		return info
	}
	if info.TrunkID == "" {
		return info
	}
	if info.Result != sip.AuthPassword {
		return info
	}

	trunks, err := s.sipClient.GetSIPInboundTrunksByIDs(ctx, []string{info.TrunkID})
	if err != nil || len(trunks) == 0 || trunks[0] == nil {
		return info
	}
	raw := strings.TrimSpace(trunks[0].GetMetadata())
	if raw == "" {
		return info
	}

	var meta trunkMetadata
	if err := json.Unmarshal([]byte(raw), &meta); err != nil {
		s.log.Debugw("failed to parse inbound trunk metadata", "sipTrunk", info.TrunkID, "err", err)
		return info
	}
	if meta.SIPEndpoint == nil {
		return info
	}

	host := strings.TrimSpace(meta.SIPEndpoint.Host)
	if host == "" {
		return info
	}
	if meta.SIPEndpoint.Port > 0 {
		info.RegisterAddr = fmt.Sprintf("%s:%d", host, meta.SIPEndpoint.Port)
	} else {
		info.RegisterAddr = host
	}
	if tr := parseSIPTransport(meta.SIPEndpoint.Transport); tr != livekit.SIPTransport_SIP_TRANSPORT_AUTO {
		info.RegisterTr = tr
	}
	if user := strings.TrimSpace(meta.AuthUser); user != "" {
		info.Username = user
	}
	return info
}

func parseSIPTransport(v string) livekit.SIPTransport {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "udp", "sip_transport_udp":
		return livekit.SIPTransport_SIP_TRANSPORT_UDP
	case "tcp", "sip_transport_tcp":
		return livekit.SIPTransport_SIP_TRANSPORT_TCP
	case "tls", "tcp_tls", "sip_transport_tls":
		return livekit.SIPTransport_SIP_TRANSPORT_TLS
	default:
		return livekit.SIPTransport_SIP_TRANSPORT_AUTO
	}
}
