package sip

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/livekit/protocol/livekit"
	lksdk "github.com/livekit/server-sdk-go/v2"
)

const inboundRegisterSyncInterval = 5 * time.Second

type inboundTrunkMetadata struct {
	SIPEndpoint *struct {
		Host      string `json:"host"`
		Port      int    `json:"port"`
		Transport string `json:"transport"`
	} `json:"sip_endpoint"`
	AuthUser string `json:"auth_user"`
}

func (s *Service) stopInboundRegisterSync() {
	s.mu.Lock()
	stop := s.stopRegSync
	s.stopRegSync = nil
	s.mu.Unlock()
	if stop != nil {
		stop()
	}
}

func (s *Service) startInboundRegisterSync() {
	if s == nil || s.conf == nil || s.srv == nil || s.cli == nil {
		return
	}
	if s.conf.WsUrl == "" || s.conf.ApiKey == "" || s.conf.ApiSecret == "" {
		return
	}
	s.stopInboundRegisterSync()

	ctx, cancel := context.WithCancel(context.Background())
	s.mu.Lock()
	s.stopRegSync = cancel
	s.mu.Unlock()

	sipClient := lksdk.NewSIPClient(s.conf.WsUrl, s.conf.ApiKey, s.conf.ApiSecret)
	seen := make(map[string]string)

	go func() {
		s.syncInboundTrunksForRegister(ctx, sipClient, seen)
		ticker := time.NewTicker(inboundRegisterSyncInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.syncInboundTrunksForRegister(ctx, sipClient, seen)
			}
		}
	}()
}

func (s *Service) syncInboundTrunksForRegister(ctx context.Context, sipClient *lksdk.SIPClient, seen map[string]string) {
	resp, err := sipClient.ListSIPInboundTrunk(ctx, &livekit.ListSIPInboundTrunkRequest{})
	if err != nil {
		s.log.Warnw("failed to list inbound trunks for REGISTER sync", err)
		return
	}
	for _, trunk := range resp.GetItems() {
		if trunk == nil {
			continue
		}
		key := trunk.GetSipTrunkId()
		fingerprint := fmt.Sprint(trunk.GetUpdatedAt()) + "|" + trunk.GetMetadata() + "|" + trunk.GetAuthUsername()
		if prev, ok := seen[key]; ok && prev == fingerprint {
			continue
		}
		seen[key] = fingerprint
		s.registerInboundTrunkFromMetadata(ctx, trunk)
	}
}

func (s *Service) registerInboundTrunkFromMetadata(ctx context.Context, trunk *livekit.SIPInboundTrunkInfo) {
	metaRaw := strings.TrimSpace(trunk.GetMetadata())
	if metaRaw == "" {
		return
	}
	var meta inboundTrunkMetadata
	if err := json.Unmarshal([]byte(metaRaw), &meta); err != nil || meta.SIPEndpoint == nil {
		return
	}
	host := strings.TrimSpace(meta.SIPEndpoint.Host)
	if host == "" {
		return
	}
	pass := trunk.GetAuthPassword()
	if pass == "" {
		return
	}
	user := strings.TrimSpace(meta.AuthUser)
	if user == "" {
		user = trunk.GetAuthUsername()
	}
	if user == "" {
		return
	}

	address := host
	if meta.SIPEndpoint.Port > 0 {
		address = fmt.Sprintf("%s:%d", host, meta.SIPEndpoint.Port)
	}
	auth := AuthInfo{
		Result:       AuthPassword,
		TrunkID:      trunk.GetSipTrunkId(),
		Username:     user,
		Password:     pass,
		RegisterAddr: address,
		RegisterTr:   transportFromMetadata(meta.SIPEndpoint.Transport),
	}
	s.srv.ensureInboundRegistered(ctx, s.log.WithValues("sipTrunk", trunk.GetSipTrunkId(), "mode", "trunk-sync"), auth)
}

func transportFromMetadata(v string) livekit.SIPTransport {
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
