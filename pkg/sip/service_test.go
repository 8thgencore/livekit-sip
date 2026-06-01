package sip

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"

	msdk "github.com/livekit/media-sdk"

	"github.com/livekit/mediatransportutil/pkg/rtcconfig"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/livekit/protocol/rpc"
	"github.com/livekit/sipgo"
	"github.com/livekit/sipgo/sip"

	"github.com/livekit/media-sdk/sdp"

	"github.com/livekit/sip/pkg/config"
	"github.com/livekit/sip/pkg/stats"
)

const (
	testPortSIPMin = 30000
	testPortSIPMax = 30050

	testPortRTPMin = 30100
	testPortRTPMax = 30150
)

func getResponseOrFail(t *testing.T, tx sip.ClientTransaction) *sip.Response {
	select {
	case <-tx.Done():
		t.Fatal("Transaction failed to complete")
	case res := <-tx.Responses():
		return res
	}

	return nil
}

func getFinalResponseOrFail(t *testing.T, tx sip.ClientTransaction, req *sip.Request) *sip.Response {
	var res *sip.Response
	for {
		res = getResponseOrFail(t, tx)
		if res.StatusCode >= 200 {
			break
		}
	}
	return res
}

func expectNoResponse(t *testing.T, tx sip.ClientTransaction) {
	select {
	case res := <-tx.Responses():
		t.Fatal("unexpected result:", res)
	case <-time.After(time.Second / 2):
		// ok
	case <-tx.Done():
		// ok
	}
}

type TestHandler struct {
	GetAuthCredentialsFunc func(ctx context.Context, call *rpc.SIPCall) (AuthInfo, error)
	DispatchCallFunc       func(ctx context.Context, info *CallInfo) CallDispatch
	OnInboundInfoFunc      func(log logger.Logger, call *rpc.SIPCall, headers Headers)
	OnSessionEndFunc       func(ctx context.Context, callIdentifier *CallIdentifier, callInfo *livekit.SIPCallInfo, reason string)
}

func (h TestHandler) GetAuthCredentials(ctx context.Context, call *rpc.SIPCall) (AuthInfo, error) {
	if h.GetAuthCredentialsFunc != nil {
		return h.GetAuthCredentialsFunc(ctx, call)
	}
	return AuthInfo{Result: AuthAccept}, nil
}

func (h TestHandler) DispatchCall(ctx context.Context, info *CallInfo) CallDispatch {
	if h.DispatchCallFunc != nil {
		return h.DispatchCallFunc(ctx, info)
	}
	identity := fmt.Sprintf("test-participant-%s", info.Call.SipCallId)
	return CallDispatch{
		Result: DispatchAccept,
		Room: RoomConfig{
			RoomName: "test-room",
			Participant: ParticipantConfig{
				Identity: identity,
				Name:     identity,
			},
		},
	}
}

func (h TestHandler) GetMediaProcessor(_ []livekit.SIPFeature, _ map[string]string, _ string, _ MediaProcessorOpts) msdk.PCM16Processor {
	return nil
}

func (h TestHandler) RegisterTransferSIPParticipantTopic(sipCallId string) error {
	// no-op
	return nil
}

func (h TestHandler) DeregisterTransferSIPParticipantTopic(sipCallId string) {
	// no-op
}

func (h TestHandler) OnInboundInfo(log logger.Logger, call *rpc.SIPCall, headers Headers) {
	if h.OnInboundInfoFunc != nil {
		h.OnInboundInfoFunc(log, call, headers)
	}
}

func (h TestHandler) OnSessionEnd(ctx context.Context, callIdentifier *CallIdentifier, callInfo *livekit.SIPCallInfo, reason string) {
	if h.OnSessionEndFunc != nil {
		h.OnSessionEndFunc(ctx, callIdentifier, callInfo, reason)
	}
}

func testInvite(t *testing.T, h Handler, hidden bool, from, to string, test func(tx sip.ClientTransaction)) {
	sipPort := rand.Intn(testPortSIPMax-testPortSIPMin) + testPortSIPMin
	localIP, err := config.GetLocalIP()
	require.NoError(t, err)

	sipServerAddress := fmt.Sprintf("%s:%d", localIP, sipPort)

	mon, err := stats.NewMonitor(&config.Config{MaxCpuUtilization: 0.9})
	require.NoError(t, err)

	// Use a no-op logger to avoid panics from async logging after test completion
	log := logger.LogRLogger(logr.Discard())
	s, err := NewService("", &config.Config{
		HideInboundPort: hidden,
		SIPPort:         sipPort,
		SIPPortListen:   sipPort,
		RTPPort:         rtcconfig.PortRange{Start: testPortRTPMin, End: testPortRTPMax},
	}, mon, log, func(projectID string) rpc.IOInfoClient { return nil })
	require.NoError(t, err)
	require.NotNil(t, s)
	t.Cleanup(s.Stop)

	s.SetHandler(h)

	require.NoError(t, s.Start())

	sipUserAgent, err := sipgo.NewUA(
		sipgo.WithUserAgent(from),
		sipgo.WithUserAgentLogger(slog.New(logger.ToSlogHandler(s.log))),
	)
	require.NoError(t, err)

	sipClient, err := sipgo.NewClient(sipUserAgent)
	require.NoError(t, err)

	offer, err := sdp.NewOfferWith(defaultCodecs, localIP, 0xB0B, sdp.EncryptionNone)
	require.NoError(t, err)
	offerData, err := offer.SDP.Marshal()
	require.NoError(t, err)

	inviteRecipent := sip.Uri{User: to, Host: sipServerAddress}
	inviteRequest := sip.NewRequest(sip.INVITE, inviteRecipent)
	inviteRequest.SetDestination(sipServerAddress)
	inviteRequest.SetBody(offerData)
	inviteRequest.AppendHeader(sip.NewHeader("Content-Type", "application/sdp"))

	tx, err := sipClient.TransactionRequest(inviteRequest)
	require.NoError(t, err)
	t.Cleanup(tx.Terminate)

	test(tx)
}

func TestService_AuthFailure(t *testing.T) {
	const (
		expectedFromUser = "foo"
		expectedToUser   = "bar"
	)
	h := &TestHandler{
		GetAuthCredentialsFunc: func(ctx context.Context, call *rpc.SIPCall) (AuthInfo, error) {
			require.Equal(t, expectedFromUser, call.From.User)
			require.Equal(t, expectedToUser, call.To.User)
			return AuthInfo{}, fmt.Errorf("Auth Failure")
		},
	}
	testInvite(t, h, false, expectedFromUser, expectedToUser, func(tx sip.ClientTransaction) {
		res := getResponseOrFail(t, tx)
		require.Equal(t, sip.StatusCode(100), res.StatusCode)

		res = getResponseOrFail(t, tx)
		require.Equal(t, sip.StatusCode(503), res.StatusCode)
	})
}

func TestService_DispatchUnavailable(t *testing.T) {
	const (
		expectedFromUser = "foo"
		expectedToUser   = "bar"
	)
	h := &TestHandler{
		GetAuthCredentialsFunc: func(ctx context.Context, call *rpc.SIPCall) (AuthInfo, error) {
			return AuthInfo{Result: AuthAccept}, nil
		},
		DispatchCallFunc: func(ctx context.Context, info *CallInfo) CallDispatch {
			return CallDispatch{Result: DispatchServiceUnavailable}
		},
	}
	testInvite(t, h, false, expectedFromUser, expectedToUser, func(tx sip.ClientTransaction) {
		res := getResponseOrFail(t, tx)
		require.Equal(t, sip.StatusCode(100), res.StatusCode)

		res = getResponseOrFail(t, tx)
		require.Equal(t, sip.StatusCode(180), res.StatusCode)

		res = getResponseOrFail(t, tx)
		require.Equal(t, sip.StatusCode(503), res.StatusCode)
	})
}

func TestService_AuthDrop(t *testing.T) {
	const (
		expectedFromUser = "foo"
		expectedToUser   = "bar"
	)
	h := &TestHandler{
		GetAuthCredentialsFunc: func(ctx context.Context, call *rpc.SIPCall) (AuthInfo, error) {
			require.Equal(t, expectedFromUser, call.From.User)
			require.Equal(t, expectedToUser, call.To.User)
			return AuthInfo{Result: AuthDrop}, nil
		},
	}
	testInvite(t, h, true, expectedFromUser, expectedToUser, func(tx sip.ClientTransaction) {
		expectNoResponse(t, tx)
	})
}

// TestService_RejectedInviteCacheReplay verifies that a second INVITE
// reusing the same Call-ID and From-tag after a final 4xx response gets
// the cached response replayed without invoking the auth/dispatch
// handlers a second time. This guards the dedup that absorbs
// provider-level retries (same Call-ID + From-tag, new SIP transaction)
// after we've already sent a terminal rejection.
func TestService_RejectedInviteCacheReplay(t *testing.T) {
	const (
		fromUser = "caller@example.com"
		toUser   = "callee@example.com"
		callID   = "rejected-invite-replay-test@example.com"
		fromTag  = "fixed-from-tag-replay"
	)

	var authCalls, dispatchCalls atomic.Int32

	h := &TestHandler{
		GetAuthCredentialsFunc: func(ctx context.Context, call *rpc.SIPCall) (AuthInfo, error) {
			authCalls.Add(1)
			return AuthInfo{Result: AuthAccept}, nil
		},
		DispatchCallFunc: func(ctx context.Context, info *CallInfo) CallDispatch {
			dispatchCalls.Add(1)
			return CallDispatch{Result: DispatchNoRuleReject}
		},
		OnSessionEndFunc: func(ctx context.Context, callIdentifier *CallIdentifier, callInfo *livekit.SIPCallInfo, reason string) {
			// no-op
		},
	}

	sipPort := rand.Intn(testPortSIPMax-testPortSIPMin) + testPortSIPMin
	localIP, err := config.GetLocalIP()
	require.NoError(t, err)
	sipServerAddress := fmt.Sprintf("%s:%d", localIP, sipPort)

	mon, err := stats.NewMonitor(&config.Config{MaxCpuUtilization: 0.9})
	require.NoError(t, err)

	log := logger.LogRLogger(logr.Discard())
	s, err := NewService("", &config.Config{
		SIPPort:       sipPort,
		SIPPortListen: sipPort,
		RTPPort:       rtcconfig.PortRange{Start: testPortRTPMin, End: testPortRTPMax},
	}, mon, log, func(projectID string) rpc.IOInfoClient { return nil })
	require.NoError(t, err)
	require.NotNil(t, s)
	s.SetHandler(h)
	require.NoError(t, s.Start())
	t.Cleanup(s.Stop)

	ua, err := sipgo.NewUA(sipgo.WithUserAgent(fromUser),
		sipgo.WithUserAgentLogger(slog.New(logger.ToSlogHandler(s.log))))
	require.NoError(t, err)
	client, err := sipgo.NewClient(ua)
	require.NoError(t, err)

	offer, err := sdp.NewOfferWith(defaultCodecs, localIP, 0xB0B, sdp.EncryptionNone)
	require.NoError(t, err)
	offerData, err := offer.SDP.Marshal()
	require.NoError(t, err)

	sendInvite := func() *sip.Response {
		recipient := sip.Uri{User: toUser, Host: sipServerAddress}
		req := sip.NewRequest(sip.INVITE, recipient)
		req.SetDestination(sipServerAddress)
		req.SetBody(offerData)
		req.AppendHeader(sip.NewHeader("Content-Type", "application/sdp"))
		req.AppendHeader(sip.NewHeader("Call-ID", callID))
		req.AppendHeader(&sip.FromHeader{
			DisplayName: fromUser,
			Address:     sip.Uri{User: fromUser, Host: sipServerAddress},
			Params:      sip.HeaderParams{{K: "tag", V: fromTag}},
		})
		tx, err := client.TransactionRequest(req)
		require.NoError(t, err)
		t.Cleanup(tx.Terminate)
		return getFinalResponseOrFail(t, tx, req)
	}

	// First INVITE: full handler invocation, 404 from DispatchNoRuleReject.
	res1 := sendInvite()
	require.Equal(t, sip.StatusCode(404), res1.StatusCode)
	require.Equal(t, int32(1), authCalls.Load())
	require.Equal(t, int32(1), dispatchCalls.Load())

	// Second INVITE with the same Call-ID + From-tag should be served from
	// the cache: same 404, but handlers must NOT be invoked again.
	res2 := sendInvite()
	require.Equal(t, sip.StatusCode(404), res2.StatusCode)
	require.Equal(t, int32(1), authCalls.Load(), "auth handler must not be re-invoked on replay")
	require.Equal(t, int32(1), dispatchCalls.Load(), "dispatch handler must not be re-invoked on replay")
}

func TestService_OnSessionEnd(t *testing.T) {
	const (
		expectedCallID    = "test-call-id"
		expectedSipCallID = "test-sip-call-id"
		expectedProjectID = "test-project"
		expectedReason    = "test-reason"
	)

	callEnded := make(chan struct{})
	var receivedCallIdentifier *CallIdentifier
	var receivedCallInfo *livekit.SIPCallInfo
	var receivedReason string

	h := &TestHandler{
		GetAuthCredentialsFunc: func(ctx context.Context, call *rpc.SIPCall) (AuthInfo, error) {
			return AuthInfo{Result: AuthAccept}, nil
		},
		DispatchCallFunc: func(ctx context.Context, info *CallInfo) CallDispatch {
			return CallDispatch{
				Result: DispatchAccept,
				Room: RoomConfig{
					RoomName: "test-room",
					Participant: ParticipantConfig{
						Identity: "test-participant",
					},
				},
			}
		},
		OnSessionEndFunc: func(ctx context.Context, callIdentifier *CallIdentifier, callInfo *livekit.SIPCallInfo, reason string) {
			receivedCallIdentifier = callIdentifier
			receivedCallInfo = callInfo
			receivedReason = reason
			close(callEnded)
		},
	}

	// Create a new service
	sipPort := rand.Intn(testPortSIPMax-testPortSIPMin) + testPortSIPMin

	mon, err := stats.NewMonitor(&config.Config{MaxCpuUtilization: 0.9})
	require.NoError(t, err)

	// Use a no-op logger to avoid panics from async logging after test completion
	log := logger.LogRLogger(logr.Discard())
	s, err := NewService("", &config.Config{
		SIPPort:       sipPort,
		SIPPortListen: sipPort,
		RTPPort:       rtcconfig.PortRange{Start: testPortRTPMin, End: testPortRTPMax},
	}, mon, log, func(projectID string) rpc.IOInfoClient { return nil })
	require.NoError(t, err)
	require.NotNil(t, s)
	t.Cleanup(s.Stop)

	s.SetHandler(h)
	require.NoError(t, s.Start())

	// Call OnSessionEnd directly with test data
	h.OnSessionEnd(context.Background(), &CallIdentifier{
		ProjectID: expectedProjectID,
		CallID:    expectedCallID,
		SipCallID: expectedSipCallID,
	}, &livekit.SIPCallInfo{
		CallId: expectedCallID,
		ParticipantAttributes: map[string]string{
			"projectID":       expectedProjectID,
			AttrSIPCallIDFull: expectedSipCallID,
		},
	}, expectedReason)

	// Wait for OnSessionEnd to be called
	select {
	case <-callEnded:
		// Success
	case <-time.After(time.Second):
		t.Fatal("OnSessionEnd was not called")
	}

	// Verify the CallIdentifier fields are correctly populated
	require.NotNil(t, receivedCallIdentifier, "CallIdentifier should not be nil")
	require.Equal(t, expectedProjectID, receivedCallIdentifier.ProjectID, "CallIdentifier.ProjectID should match")
	require.Equal(t, expectedCallID, receivedCallIdentifier.CallID, "CallIdentifier.CallID should match")
	require.Equal(t, expectedSipCallID, receivedCallIdentifier.SipCallID, "CallIdentifier.SipCallID should match")

	// Verify the CallInfo fields
	require.NotNil(t, receivedCallInfo, "CallInfo should not be nil")
	require.Equal(t, expectedProjectID, receivedCallInfo.ParticipantAttributes["projectID"], "CallInfo.ParticipantAttributes[projectID] should match")
	require.Equal(t, expectedCallID, receivedCallInfo.CallId, "CallInfo.CallId should match")
	require.Equal(t, expectedSipCallID, receivedCallInfo.ParticipantAttributes[AttrSIPCallIDFull], "CallInfo.ParticipantAttributes[sip.callIDFull] should match")
	require.Equal(t, expectedReason, receivedReason, "Reason should match")
}

func TestRegisteredInboundTrunkSkipsInviteDigestAuth(t *testing.T) {
	const (
		fromUser = "caller@example.com"
		toUser   = "registered@example.com"
		username = "register-user"
		password = "register-pass"
		callID   = "registered-inbound@test.com"
	)

	log := logger.NewTestLoggerLevel(t, 1)

	registrar := newUATest(t, log, netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), 9))
	registerSink := registrar.RegisterSink("", "REGISTER")
	registerSeen := make(chan struct{}, 1)
	go func() {
		select {
		case msg := <-registerSink:
			registerSeen <- struct{}{}
			res := sip.NewResponseFromRequest(msg.req, sip.StatusOK, "OK", nil)
			_ = msg.tx.Respond(res)
		case <-time.After(5 * time.Second):
		}
	}()

	var dispatchCalls int
	var dispatchMu sync.Mutex
	h := &TestHandler{
		GetAuthCredentialsFunc: func(ctx context.Context, call *rpc.SIPCall) (AuthInfo, error) {
			return AuthInfo{
				Result:       AuthPassword,
				Auth:         InboundAuth{Username: username, Password: password},
				RegisterAddr: registrar.localAddr.String(),
				RegisterTr:   livekit.SIPTransport_SIP_TRANSPORT_UDP,
			}, nil
		},
		DispatchCallFunc: func(ctx context.Context, info *CallInfo) CallDispatch {
			dispatchMu.Lock()
			dispatchCalls++
			dispatchMu.Unlock()
			return CallDispatch{Result: DispatchNoRuleReject}
		},
		OnSessionEndFunc: func(ctx context.Context, callIdentifier *CallIdentifier, callInfo *livekit.SIPCallInfo, reason string) {
		},
	}

	sipPort := rand.Intn(testPortSIPMax-testPortSIPMin) + testPortSIPMin
	localIP, err := config.GetLocalIP()
	require.NoError(t, err)
	sipServerAddress := fmt.Sprintf("%s:%d", localIP, sipPort)

	mon, err := stats.NewMonitor(&config.Config{MaxCpuUtilization: 0.9})
	require.NoError(t, err)

	s, err := NewService("", &config.Config{
		HideInboundPort: false,
		SIPPort:         sipPort,
		SIPPortListen:   sipPort,
		RTPPort:         rtcconfig.PortRange{Start: testPortRTPMin, End: testPortRTPMax},
	}, mon, log, func(projectID string) rpc.IOInfoClient { return nil })
	require.NoError(t, err)
	require.NotNil(t, s)

	s.SetHandler(h)
	require.NoError(t, s.Start())
	t.Cleanup(s.Stop)

	sipUserAgent, err := sipgo.NewUA(
		sipgo.WithUserAgent(fromUser),
		sipgo.WithUserAgentLogger(slog.New(logger.ToSlogHandler(s.log))),
	)
	require.NoError(t, err)
	t.Cleanup(func() { sipUserAgent.Close() })

	sipClient, err := sipgo.NewClient(sipUserAgent)
	require.NoError(t, err)
	t.Cleanup(func() { sipClient.Close() })

	offer, err := sdp.NewOffer(localIP, 0xB0B, sdp.EncryptionNone)
	require.NoError(t, err)
	offerData, err := offer.SDP.Marshal()
	require.NoError(t, err)

	inviteRecipient := sip.Uri{User: toUser, Host: sipServerAddress}
	inviteRequest := sip.NewRequest(sip.INVITE, inviteRecipient)
	inviteRequest.SetDestination(sipServerAddress)
	inviteRequest.SetBody(offerData)
	inviteRequest.AppendHeader(sip.NewHeader("Content-Type", "application/sdp"))
	inviteRequest.AppendHeader(sip.NewHeader("Call-ID", callID))
	inviteRequest.AppendHeader(&sip.FromHeader{
		DisplayName: fromUser,
		Address:     sip.Uri{User: fromUser, Host: sipServerAddress},
		Params:      sip.HeaderParams{{"tag", "registered-inbound-from-tag"}},
	})

	tx, err := sipClient.TransactionRequest(inviteRequest)
	require.NoError(t, err)
	t.Cleanup(tx.Terminate)

	res := getResponseOrFail(t, tx)
	require.Equal(t, sip.StatusCode(100), res.StatusCode)
	res = getFinalResponseOrFail(t, tx, inviteRequest)
	require.NotEqual(t, sip.StatusCode(407), res.StatusCode)
	require.Nil(t, res.GetHeader("Proxy-Authenticate"))
	require.Equal(t, sip.StatusNotFound, res.StatusCode)

	select {
	case <-registerSeen:
	case <-time.After(time.Second):
		t.Fatal("expected inbound REGISTER to be ensured")
	}

	dispatchMu.Lock()
	require.Equal(t, 1, dispatchCalls)
	dispatchMu.Unlock()
}

func TestPasswordInboundTrunkSkipsInviteDigestAuth(t *testing.T) {
	const (
		fromUser = "caller@example.com"
		toUser   = "number@example.com"
		username = "inbound-user"
		password = "inbound-pass"
		callID   = "password-inbound@test.com"
	)

	log := logger.NewTestLoggerLevel(t, 1)

	h := &TestHandler{
		GetAuthCredentialsFunc: func(ctx context.Context, call *rpc.SIPCall) (AuthInfo, error) {
			return AuthInfo{
				Result: AuthPassword,
				Auth:   InboundAuth{Username: username, Password: password},
				ProviderInfo: &livekit.ProviderInfo{
					Id:   "generic",
					Name: "Generic provider",
				},
			}, nil
		},
		DispatchCallFunc: func(ctx context.Context, info *CallInfo) CallDispatch {
			return CallDispatch{Result: DispatchNoRuleReject}
		},
		OnSessionEndFunc: func(ctx context.Context, callIdentifier *CallIdentifier, callInfo *livekit.SIPCallInfo, reason string) {
		},
	}

	sipPort := rand.Intn(testPortSIPMax-testPortSIPMin) + testPortSIPMin
	localIP, err := config.GetLocalIP()
	require.NoError(t, err)
	sipServerAddress := fmt.Sprintf("%s:%d", localIP, sipPort)

	mon, err := stats.NewMonitor(&config.Config{MaxCpuUtilization: 0.9})
	require.NoError(t, err)

	s, err := NewService("", &config.Config{
		HideInboundPort: false,
		SIPPort:         sipPort,
		SIPPortListen:   sipPort,
		RTPPort:         rtcconfig.PortRange{Start: testPortRTPMin, End: testPortRTPMax},
	}, mon, log, func(projectID string) rpc.IOInfoClient { return nil })
	require.NoError(t, err)
	require.NotNil(t, s)

	s.SetHandler(h)
	require.NoError(t, s.Start())
	t.Cleanup(s.Stop)

	sipUserAgent, err := sipgo.NewUA(
		sipgo.WithUserAgent(fromUser),
		sipgo.WithUserAgentLogger(slog.New(logger.ToSlogHandler(s.log))),
	)
	require.NoError(t, err)
	t.Cleanup(func() { sipUserAgent.Close() })

	sipClient, err := sipgo.NewClient(sipUserAgent)
	require.NoError(t, err)
	t.Cleanup(func() { sipClient.Close() })

	offer, err := sdp.NewOffer(localIP, 0xB0B, sdp.EncryptionNone)
	require.NoError(t, err)
	offerData, err := offer.SDP.Marshal()
	require.NoError(t, err)

	inviteRecipient := sip.Uri{User: toUser, Host: sipServerAddress}
	inviteRequest := sip.NewRequest(sip.INVITE, inviteRecipient)
	inviteRequest.SetDestination(sipServerAddress)
	inviteRequest.SetBody(offerData)
	inviteRequest.AppendHeader(sip.NewHeader("Content-Type", "application/sdp"))
	inviteRequest.AppendHeader(sip.NewHeader("Call-ID", callID))
	inviteRequest.AppendHeader(&sip.FromHeader{
		DisplayName: fromUser,
		Address:     sip.Uri{User: fromUser, Host: sipServerAddress},
		Params:      sip.HeaderParams{{"tag", "password-inbound-from-tag"}},
	})

	tx, err := sipClient.TransactionRequest(inviteRequest)
	require.NoError(t, err)
	t.Cleanup(tx.Terminate)

	res := getResponseOrFail(t, tx)
	require.Equal(t, sip.StatusCode(100), res.StatusCode)
	res = getFinalResponseOrFail(t, tx, inviteRequest)
	require.NotEqual(t, sip.StatusCode(407), res.StatusCode)
	require.Nil(t, res.GetHeader("Proxy-Authenticate"))
	require.Equal(t, sip.StatusNotFound, res.StatusCode)
}

// When a cancel request is sent, we expect two responses, 200 (for CANCEL), and 487 (for INVITE).
// This test makes sure the 487 response is received (can't test CANCEL-200)
func TestCANCELSendsBothResponses(t *testing.T) {
	const (
		fromUser = "caller@example.com"
		toUser   = "callee@example.com"
	)

	st := NewServiceTest(t, &serviceTestConfig{GetRoom: newTestRoomConfig(&testRoomConfig{ringForever: true})})
	loopback := netip.MustParseAddr("127.0.0.1")
	sipServerAddress := st.Address()

	// Create SIP client using sipgo
	sipUserAgent, err := sipgo.NewUA(
		sipgo.WithUserAgent(fromUser),
	)
	require.NoError(t, err)

	sipClient, err := sipgo.NewClient(sipUserAgent)
	require.NoError(t, err)

	// Create SDP offer
	offer, err := sdp.NewOfferWith(defaultCodecs, loopback, 0xB0B, sdp.EncryptionNone)
	require.NoError(t, err)
	offerData, err := offer.SDP.Marshal()
	require.NoError(t, err)

	// Create INVITE request
	inviteRecipient := sip.Uri{User: toUser, Host: sipServerAddress}
	inviteRequest := sip.NewRequest(sip.INVITE, inviteRecipient)
	inviteRequest.SetDestination(sipServerAddress)
	inviteRequest.SetBody(offerData)
	inviteRequest.AppendHeader(sip.NewHeader("Content-Type", "application/sdp"))

	// Send INVITE
	tx, err := sipClient.TransactionRequest(inviteRequest)
	require.NoError(t, err)
	t.Cleanup(tx.Terminate)

	// Wait for 100 Trying
	res100 := getResponseOrFail(t, tx)
	require.Equal(t, sip.StatusCode(100), res100.StatusCode, "Should receive 100 Trying")

	// Wait for 180 Ringing (call is now ringing)
	res180 := getResponseOrFail(t, tx)
	require.Equal(t, sip.StatusCode(180), res180.StatusCode, "Should receive 180 Ringing")

	// Now send CANCEL
	err = tx.Cancel()
	require.NoError(t, err, "Should be able to send CANCEL")

	// On-the-wire there should be two responses after CANCEL:
	// 1. 200 OK response to the CANCEL request (CSeq method = CANCEL)
	// 2. 487 Request Terminated response to the original INVITE (CSeq method = INVITE)
	//    This is the critical one - we must receive it
	// However, the 200 OK response to CANCEL will not come through tx.Responses().
	// Sipgo treats both INVITE and CANCEL as the same transaction, and has special handling
	// to swallow the 200 OK response to CANCEL, so it can't look like the INVITE got the 200.

	// Collect responses until we get the final 487 or transaction completes
	var responses []*sip.Response

	// Wait for responses with a timeout
	timeout := time.After(time.Second)

	// Collect responses until we get 487 or timeout
	for {
		select {
		case res := <-tx.Responses():
			responses = append(responses, res)
			cseq := res.CSeq()

			// Debug: log all responses to understand what we're receiving
			cseqMethod := "nil"
			if cseq != nil {
				cseqMethod = string(cseq.MethodName)
			}
			t.Logf("Received response: StatusCode=%d, CSeq method=%s", res.StatusCode, cseqMethod)

			if res.StatusCode < 200 {
				continue
			}
			require.Equal(t, sip.StatusCode(487), res.StatusCode, "Should have received 487 Request Terminated response to INVITE when CANCEL is sent")
			require.NotNil(t, cseq, "487 response should have CSeq header")
			require.Equal(t, sip.INVITE, cseq.MethodName, "487 response should be for INVITE method")
			return // Success!

		case <-tx.Done():
			t.Fatal("Transaction done without receiving expected 487 response")

		case <-timeout:
			// Log all received responses for debugging
			t.Logf("Timeout after receiving %d responses", len(responses))
			for i, res := range responses {
				cseq := res.CSeq()
				cseqMethod := "nil"
				if cseq != nil {
					cseqMethod = string(cseq.MethodName)
				}
				t.Logf("  Response %d: StatusCode=%d, CSeq method=%s", i+1, res.StatusCode, cseqMethod)
			}
			t.Fatal("Timeout waiting for 487 Request Terminated response after CANCEL")
		}
	}
}

// newServiceForAffinity creates a minimal Service with initialized client/server maps
// suitable for testing CreateSIPParticipantAffinity without network setup.
func newServiceForAffinity(conf *config.Config) *Service {
	cli := &Client{
		conf:        conf,
		activeCalls: make(map[LocalTag]*outboundCall),
	}
	srv := &Server{
		conf:       conf,
		byLocalTag: make(map[LocalTag]*inboundCall),
	}
	return &Service{
		conf: conf,
		cli:  cli,
		srv:  srv,
	}
}

func TestCreateSIPParticipantAffinity_NoConfig_NoCalls(t *testing.T) {
	s := newServiceForAffinity(&config.Config{})
	got := s.CreateSIPParticipantAffinity(context.Background(), nil)
	// 1 / (1 + 0) = 1.0
	require.InDelta(t, float32(1.0), got, 0.001)
}

func TestCreateSIPParticipantAffinity_NoConfig_WithCalls(t *testing.T) {
	s := newServiceForAffinity(&config.Config{})

	// Add 4 outbound calls
	for i := 0; i < 4; i++ {
		s.cli.activeCalls[LocalTag(fmt.Sprintf("out-%d", i))] = &outboundCall{}
	}
	// Add 5 inbound calls
	for i := 0; i < 5; i++ {
		s.srv.byLocalTag[LocalTag(fmt.Sprintf("in-%d", i))] = &inboundCall{}
	}

	got := s.CreateSIPParticipantAffinity(context.Background(), nil)
	// 1 / (1 + 9) = 0.1
	require.InDelta(t, float32(0.1), got, 0.001)
}

func TestCreateSIPParticipantAffinity_WithMaxCalls(t *testing.T) {
	s := newServiceForAffinity(&config.Config{MaxActiveCalls: 100})

	got := s.CreateSIPParticipantAffinity(context.Background(), nil)
	// 0 active, max 100 => 1 - 0/100 = 1.0
	require.InDelta(t, float32(1.0), got, 0.001)
}

func TestCreateSIPParticipantAffinity_WithMaxCalls_PartialLoad(t *testing.T) {
	s := newServiceForAffinity(&config.Config{MaxActiveCalls: 100})

	// Add 25 outbound calls before first measurement
	for i := 0; i < 25; i++ {
		s.cli.activeCalls[LocalTag(fmt.Sprintf("out-%d", i))] = &outboundCall{}
	}
	got := s.CreateSIPParticipantAffinity(context.Background(), nil)
	// 25 active, max 100 => 1 - 25/100 = 0.75
	require.InDelta(t, float32(0.75), got, 0.001)

	// Add 25 more (50 total)
	for i := 25; i < 50; i++ {
		s.cli.activeCalls[LocalTag(fmt.Sprintf("out-%d", i))] = &outboundCall{}
	}
	got = s.CreateSIPParticipantAffinity(context.Background(), nil)
	// 50 active, max 100 => 1 - 50/100 = 0.5
	require.InDelta(t, float32(0.5), got, 0.001)

	// Add 49 more (99 total, just under capacity)
	for i := 50; i < 99; i++ {
		s.cli.activeCalls[LocalTag(fmt.Sprintf("out-%d", i))] = &outboundCall{}
	}
	got = s.CreateSIPParticipantAffinity(context.Background(), nil)
	// 99 active, max 100 => 1 - 99/100 = 0.01
	require.InDelta(t, float32(0.01), got, 0.001)
}

func TestCreateSIPParticipantAffinity_AtCapacity(t *testing.T) {
	s := newServiceForAffinity(&config.Config{MaxActiveCalls: 10})

	for i := 0; i < 10; i++ {
		s.cli.activeCalls[LocalTag(fmt.Sprintf("out-%d", i))] = &outboundCall{}
	}

	got := s.CreateSIPParticipantAffinity(context.Background(), nil)
	require.Equal(t, float32(0), got)
}

func TestCreateSIPParticipantAffinity_OverCapacity(t *testing.T) {
	s := newServiceForAffinity(&config.Config{MaxActiveCalls: 10})

	for i := 0; i < 15; i++ {
		s.cli.activeCalls[LocalTag(fmt.Sprintf("out-%d", i))] = &outboundCall{}
	}

	got := s.CreateSIPParticipantAffinity(context.Background(), nil)
	require.Equal(t, float32(0), got)
}

func TestCreateSIPParticipantAffinity_MixedInboundOutbound(t *testing.T) {
	s := newServiceForAffinity(&config.Config{MaxActiveCalls: 20})

	// 6 outbound + 4 inbound = 10 total
	for i := 0; i < 6; i++ {
		s.cli.activeCalls[LocalTag(fmt.Sprintf("out-%d", i))] = &outboundCall{}
	}
	for i := 0; i < 4; i++ {
		s.srv.byLocalTag[LocalTag(fmt.Sprintf("in-%d", i))] = &inboundCall{}
	}

	got := s.CreateSIPParticipantAffinity(context.Background(), nil)
	// 10 active, max 20 => 1 - 10/20 = 0.5
	require.InDelta(t, float32(0.5), got, 0.001)
}

func TestCreateSIPParticipantAffinity_TrunkWhitelist_Allowed(t *testing.T) {
	s := newServiceForAffinity(&config.Config{
		SIPTrunkIds: []string{"trunk-a", "trunk-b"},
	})

	req := &rpc.InternalCreateSIPParticipantRequest{SipTrunkId: "trunk-a"}
	got := s.CreateSIPParticipantAffinity(context.Background(), req)
	// Trunk is whitelisted, 0 active calls, no max => 1/(1+0) = 1.0
	require.InDelta(t, float32(1.0), got, 0.001)
}

func TestCreateSIPParticipantAffinity_TrunkWhitelist_Rejected(t *testing.T) {
	s := newServiceForAffinity(&config.Config{
		SIPTrunkIds: []string{"trunk-a", "trunk-b"},
	})

	req := &rpc.InternalCreateSIPParticipantRequest{SipTrunkId: "trunk-c"}
	got := s.CreateSIPParticipantAffinity(context.Background(), req)
	require.Equal(t, float32(0), got)
}

func TestCreateSIPParticipantAffinity_TrunkWhitelist_EmptyTrunkId(t *testing.T) {
	s := newServiceForAffinity(&config.Config{
		SIPTrunkIds: []string{"trunk-a"},
	})

	req := &rpc.InternalCreateSIPParticipantRequest{}
	got := s.CreateSIPParticipantAffinity(context.Background(), req)
	// Empty trunk ID is not in the whitelist
	require.Equal(t, float32(0), got)
}

func TestCreateSIPParticipantAffinity_TrunkWhitelist_EmptyList(t *testing.T) {
	s := newServiceForAffinity(&config.Config{})

	// No whitelist configured, any trunk ID should work
	req := &rpc.InternalCreateSIPParticipantRequest{SipTrunkId: "any-trunk"}
	got := s.CreateSIPParticipantAffinity(context.Background(), req)
	require.InDelta(t, float32(1.0), got, 0.001)
}

func TestCreateSIPParticipantAffinity_TrunkWhitelist_WithMaxCalls(t *testing.T) {
	s := newServiceForAffinity(&config.Config{
		SIPTrunkIds:    []string{"trunk-a"},
		MaxActiveCalls: 100,
	})

	// Add 50 calls
	for i := 0; i < 50; i++ {
		s.cli.activeCalls[LocalTag(fmt.Sprintf("out-%d", i))] = &outboundCall{}
	}

	// Whitelisted trunk: should get normal affinity
	req := &rpc.InternalCreateSIPParticipantRequest{SipTrunkId: "trunk-a"}
	got := s.CreateSIPParticipantAffinity(context.Background(), req)
	require.InDelta(t, float32(0.5), got, 0.001)

	// Non-whitelisted trunk: 0 regardless of load
	req = &rpc.InternalCreateSIPParticipantRequest{SipTrunkId: "trunk-x"}
	got = s.CreateSIPParticipantAffinity(context.Background(), req)
	require.Equal(t, float32(0), got)
}
