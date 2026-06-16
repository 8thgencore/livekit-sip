package sip

import (
	"context"
	"encoding/json"
	"hash/fnv"
	"strconv"
	"strings"
	"time"

	"github.com/livekit/protocol/livekit"
	lksdk "github.com/livekit/server-sdk-go/v2"
)

const inboundRegisterSyncInterval = 5 * time.Second
const inboundRegisterEnsureInterval = 2 * time.Minute
const maxAuthRegisterRetryError = "max auth retry attempts reached for SIP register"

type inboundRegisterSIPClient interface {
	ListSIPInboundTrunk(ctx context.Context, in *livekit.ListSIPInboundTrunkRequest) (*livekit.ListSIPInboundTrunkResponse, error)
	DeleteSIPTrunk(ctx context.Context, in *livekit.DeleteSIPTrunkRequest) (*livekit.SIPTrunkInfo, error)
}

type inboundRegisterSyncState struct {
	fingerprint string
	nextEnsure  time.Time
}

type inboundTrunkMetadata struct {
	SIPEndpoint *struct {
		Host           string `json:"host"`
		Port           int    `json:"port"`
		Transport      string `json:"transport"`
		IdentityDomain string `json:"identity_domain"`
		FromHost       string `json:"from_host"`
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
	states := make(map[string]inboundRegisterSyncState)

	go func() {
		s.syncInboundTrunksForRegister(ctx, sipClient, states)
		ticker := time.NewTicker(inboundRegisterSyncInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.syncInboundTrunksForRegister(ctx, sipClient, states)
			}
		}
	}()
}

func (s *Service) syncInboundTrunksForRegister(ctx context.Context, sipClient inboundRegisterSIPClient, states map[string]inboundRegisterSyncState) {
	resp, err := sipClient.ListSIPInboundTrunk(ctx, &livekit.ListSIPInboundTrunkRequest{})
	if err != nil {
		s.log.Warnw("failed to list inbound trunks for REGISTER sync", err)
		return
	}

	now := time.Now()
	nextTickAt := now.Add(inboundRegisterSyncInterval)
	active := make(map[string]struct{}, len(resp.GetItems()))
	for _, trunk := range resp.GetItems() {
		if trunk == nil {
			continue
		}
		id := trunk.GetSipTrunkId()
		if id == "" {
			continue
		}
		active[id] = struct{}{}

		fingerprint := trunk.GetMetadata() + "|" + trunk.GetAuthUsername() + "|" + trunk.GetAuthPassword()
		st := states[id]
		changed := st.fingerprint != fingerprint
		// Refresh proactively: if the trunk would become due by the next sync tick,
		// run ensure now instead of waiting until it is already overdue.
		if !changed && nextTickAt.Before(st.nextEnsure) {
			continue
		}
		if err := s.registerInboundTrunkFromMetadata(ctx, trunk); isMaxAuthRegisterRetry(err) {
			s.deleteInboundTrunkAfterRegisterAuthFailure(ctx, sipClient, id, err)
			delete(active, id)
			delete(states, id)
			continue
		}
		states[id] = inboundRegisterSyncState{
			fingerprint: fingerprint,
			nextEnsure:  now.Add(inboundRegisterEnsureInterval + inboundRegisterJitter(id)),
		}
	}

	for id := range states {
		if _, ok := active[id]; !ok {
			delete(states, id)
		}
	}
}

func inboundRegisterJitter(trunkID string) time.Duration {
	h := fnv.New32a()
	_, _ = h.Write([]byte(trunkID))
	// 0..29s deterministic jitter to spread periodic ensure across trunks.
	return time.Duration(h.Sum32()%30) * time.Second
}

func (s *Service) registerInboundTrunkFromMetadata(ctx context.Context, trunk *livekit.SIPInboundTrunkInfo) error {
	metaRaw := strings.TrimSpace(trunk.GetMetadata())
	if metaRaw == "" {
		return nil
	}
	var meta inboundTrunkMetadata
	if err := json.Unmarshal([]byte(metaRaw), &meta); err != nil || meta.SIPEndpoint == nil {
		return nil
	}
	host := strings.TrimSpace(meta.SIPEndpoint.Host)
	if host == "" {
		return nil
	}
	pass := trunk.GetAuthPassword()
	if pass == "" {
		return nil
	}
	user := strings.TrimSpace(meta.AuthUser)
	if user == "" {
		user = trunk.GetAuthUsername()
	}
	if user == "" {
		return nil
	}

	address := host
	if meta.SIPEndpoint.Port > 0 {
		address = host + ":" + strconv.Itoa(meta.SIPEndpoint.Port)
	}
	auth := AuthInfo{
		Result:           AuthPassword,
		TrunkID:          trunk.GetSipTrunkId(),
		Auth:             InboundAuth{Username: user, Password: pass},
		RegisterAddr:     address,
		RegisterFromHost: firstMetadataValue(meta.SIPEndpoint.IdentityDomain, meta.SIPEndpoint.FromHost),
		RegisterTr:       transportFromMetadata(meta.SIPEndpoint.Transport),
	}
	_, err := s.srv.ensureInboundRegistered(ctx, s.log.WithValues("sipTrunk", trunk.GetSipTrunkId(), "mode", "trunk-sync"), auth)
	return err
}

func isMaxAuthRegisterRetry(err error) bool {
	return err != nil && strings.Contains(err.Error(), maxAuthRegisterRetryError)
}

func (s *Service) deleteInboundTrunkAfterRegisterAuthFailure(ctx context.Context, sipClient inboundRegisterSIPClient, trunkID string, cause error) {
	deleteCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if _, err := sipClient.DeleteSIPTrunk(deleteCtx, &livekit.DeleteSIPTrunkRequest{SipTrunkId: trunkID}); err != nil {
		s.log.Warnw("failed to delete inbound SIP trunk after REGISTER auth failure", err,
			"sipTrunk", trunkID,
			"cause", cause,
		)
		return
	}
	s.log.Infow("deleted inbound SIP trunk after REGISTER auth failure",
		"sipTrunk", trunkID,
		"cause", cause,
	)
}

func firstMetadataValue(values ...string) string {
	for _, value := range values {
		if normalized := strings.TrimSpace(value); normalized != "" {
			return normalized
		}
	}
	return ""
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
