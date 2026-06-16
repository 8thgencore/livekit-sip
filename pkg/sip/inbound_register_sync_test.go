package sip

import (
	"context"
	"testing"

	"github.com/icholy/digest"
	"github.com/stretchr/testify/require"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/livekit/sipgo/sip"
)

type fakeInboundRegisterSIPClient struct {
	trunks  []*livekit.SIPInboundTrunkInfo
	deleted []string
}

func (f *fakeInboundRegisterSIPClient) ListSIPInboundTrunk(context.Context, *livekit.ListSIPInboundTrunkRequest) (*livekit.ListSIPInboundTrunkResponse, error) {
	return &livekit.ListSIPInboundTrunkResponse{Items: f.trunks}, nil
}

func (f *fakeInboundRegisterSIPClient) DeleteSIPTrunk(_ context.Context, req *livekit.DeleteSIPTrunkRequest) (*livekit.SIPTrunkInfo, error) {
	f.deleted = append(f.deleted, req.GetSipTrunkId())
	return &livekit.SIPTrunkInfo{SipTrunkId: req.GetSipTrunkId()}, nil
}

func TestSyncInboundTrunksDeletesTrunkAfterRegisterMaxAuthRetry(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)

	svc := &Service{
		log: logger.NewTestLogger(t),
		srv: &Server{cli: client},
	}
	trunkClient := &fakeInboundRegisterSIPClient{
		trunks: []*livekit.SIPInboundTrunkInfo{
			{
				SipTrunkId:   "ST_register_auth_loop",
				AuthUsername: "unused-auth-user",
				AuthPassword: mockAuthPassword,
				Metadata: `{
					"sip_endpoint": {
						"host": "registrar.example.com",
						"port": 5060,
						"transport": "udp",
						"identity_domain": "example.com"
					},
					"auth_user": "test-auth-user"
				}`,
			},
		},
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		svc.syncInboundTrunksForRegister(context.Background(), trunkClient, map[string]inboundRegisterSyncState{})
	}()

	for attempt := 0; attempt < registerAuthMaxAttempts; attempt++ {
		tx := waitTransaction(t, sipClient)
		require.Equal(t, sip.REGISTER, tx.req.Method)
		challenge := digest.Challenge{
			Realm: "registrar.example.com",
			Nonce: "nonce",
		}
		unauthorized := sip.NewResponseFromRequest(tx.req, sip.StatusUnauthorized, "Unauthorized", nil)
		unauthorized.AppendHeader(sip.NewHeader("WWW-Authenticate", challenge.String()))
		require.NoError(t, tx.transaction.SendResponse(unauthorized))
	}

	<-done
	require.Equal(t, []string{"ST_register_auth_loop"}, trunkClient.deleted)
}
