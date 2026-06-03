// Copyright 2023 LiveKit, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sip

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/icholy/digest"
	"github.com/livekit/media-sdk/g711"
	"github.com/livekit/media-sdk/g722"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/livekit/psrpc"
	"github.com/livekit/sip/pkg/stats"
	"github.com/livekit/sipgo/sip"
)

func setOutboundRegisterMode(req interface{ ProtoReflect() protoreflect.Message }, mode outboundRegisterMode) {
	msg := req.ProtoReflect()
	unknown := msg.GetUnknown()
	unknown = protowire.AppendTag(unknown, internalCreateSIPParticipantRegisterModeField, protowire.VarintType)
	unknown = protowire.AppendVarint(unknown, uint64(mode))
	msg.SetUnknown(unknown)
}

func TestOutboundRegisterModeDefaultsToRequired(t *testing.T) {
	require.Equal(t, outboundRegisterModeRequired, outboundRegisterModeFromRequest(nil))
	require.Equal(t, outboundRegisterModeRequired, outboundRegisterModeFromRequest(MinimalCreateSIPParticipantRequest()))
	require.Equal(t, outboundRegisterModeRequired, normalizeOutboundRegisterMode(99))
}

func TestOutboundRegisterModeHonorsExplicitAutoAndDisabled(t *testing.T) {
	req := MinimalCreateSIPParticipantRequest()
	setOutboundRegisterMode(req, outboundRegisterModeAuto)
	require.Equal(t, outboundRegisterModeAuto, outboundRegisterModeFromRequest(req))

	req = MinimalCreateSIPParticipantRequest()
	setOutboundRegisterMode(req, outboundRegisterModeDisabled)
	require.Equal(t, outboundRegisterModeDisabled, outboundRegisterModeFromRequest(req))
}

func newTestOutboundCall(client *Client) *outboundCall {
	contact := client.ContactURI(TransportUDP)
	from := URI{
		User:      "0101536",
		Host:      client.sconf.SignalingIP.String(),
		Addr:      contact.Addr,
		Transport: TransportUDP,
	}
	call := &outboundCall{
		c:       client,
		log:     logger.GetLogger(),
		sipConf: sipOutboundConfig{user: "0101536", pass: "test-password"},
		mon:     client.mon.NewCall(stats.Outbound, "sip.novofon.ru", "sip.novofon.ru:5060"),
	}
	call.cc = client.newOutbound(call.log, LocalTag("test-call-id"), from, contact, nil, nil)
	return call
}

func TestClientGetActiveCallForRequestFallsBackToCallID(t *testing.T) {
	client := &Client{
		activeCalls: make(map[LocalTag]*outboundCall),
	}
	from := URI{User: "0101536", Host: "127.0.0.1", Transport: TransportUDP}
	contact := URI{User: "0101536", Host: "127.0.0.1", Transport: TransportUDP}
	call := &outboundCall{}
	call.cc = client.newOutbound(logger.GetLogger(), LocalTag("test-call-id"), from, contact, nil, nil)
	call.cc.callID = "provider-call-id"

	client.cmu.Lock()
	client.activeCalls[call.cc.ID()] = call
	client.cmu.Unlock()

	req := sip.NewRequest(sip.BYE, sip.Uri{Host: "sip.provider.example"})
	req.AppendHeader(sip.NewHeader("Call-ID", "provider-call-id"))
	req.AppendHeader(&sip.FromHeader{
		Address: sip.Uri{User: "remote", Host: "sip.provider.example"},
		Params:  sip.HeaderParams{{K: "tag", V: "remote-tag"}},
	})
	req.AppendHeader(&sip.ToHeader{
		Address: sip.Uri{User: "0101536", Host: "127.0.0.1"},
	})

	matchedCall, matchedBy := client.getActiveCallForRequest(req)

	require.Same(t, call, matchedCall)
	require.Equal(t, "call-id", matchedBy)
}

func TestSIPResponseContextTimeoutReturnsSIPRequestTimeout(t *testing.T) {
	tx := &testSIPClientTransaction{
		log:       logger.GetLogger(),
		responses: make(chan *sip.Response),
		cancels:   make(chan struct{}, 1),
		done:      make(chan struct{}),
		err:       make(chan error),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	resp, err := sipResponse(ctx, tx, make(chan struct{}), nil)

	require.Nil(t, resp)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrSIPRequestTimeout)
	var sipErr *livekit.SIPStatus
	require.ErrorAs(t, err, &sipErr)
	require.Equal(t, livekit.SIPStatusCode_SIP_STATUS_REQUEST_TIMEOUT, sipErr.Code)
	require.Equal(t, "Request Timeout", sipErr.Status)
	select {
	case <-tx.cancels:
	default:
		t.Fatal("expected SIP transaction cancellation")
	}
}

func sendProxyAuthRequired(t *testing.T, tx *transactionRequest) {
	t.Helper()
	challenge := digest.Challenge{
		Realm: "sip.nvfn.ru",
		Nonce: "12345678901234567890123456789012",
	}
	resp := sip.NewResponseFromRequest(tx.req, sip.StatusProxyAuthRequired, "Proxy Authentication Required", nil)
	resp.AppendHeader(sip.NewHeader("Proxy-Authenticate", challenge.String()))
	sendTestResponse(t, tx, resp)
}

func sendTestResponse(t *testing.T, tx *transactionRequest, resp *sip.Response) {
	t.Helper()
	deadline := time.Now().Add(200 * time.Millisecond)
	for {
		err := tx.transaction.SendResponse(resp)
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			require.NoError(t, err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestOutboundProviderProfileResolution(t *testing.T) {
	telphin := outboundProviderProfileForAddress("teleo.telphin.ru:5068")
	require.Equal(t, "telphin", telphin.ID)
	require.False(t, telphin.SkipRegistrationInAuto)
	require.True(t, telphin.AllowRegisteredInviteDirectFallback)
	require.True(t, telphin.DefaultG711Only)

	telphinSIP := outboundProviderProfileForAddress("vip1.sip.telphin.com:5060")
	require.Equal(t, "telphin", telphinSIP.ID)
	require.True(t, telphinSIP.DefaultG711Only)

	novofon := outboundProviderProfileForAddress("SIP.NOVOFON.RU:5060")
	require.Equal(t, "novofon", novofon.ID)
	require.True(t, novofon.SkipRegistrationInAuto)
	require.True(t, novofon.DirectAuthFailureIsConfigError)
	require.True(t, novofon.DefaultG711Only)

	novofonSubdomain := outboundProviderProfileForAddress("proxy.sip.novofon.ru:5060")
	require.Equal(t, "novofon", novofonSubdomain.ID)
	require.True(t, novofonSubdomain.SkipRegistrationInAuto)

	uiscom := outboundProviderProfileForAddress("pbx.uiscom.ru.")
	require.Equal(t, "uiscom", uiscom.ID)
	require.False(t, uiscom.SkipRegistrationInAuto)
	require.True(t, uiscom.AllowRegisteredInviteDirectFallback)
	require.False(t, uiscom.DeleteTrunkAfterCall)
	require.True(t, uiscom.DefaultG711Only)
	require.False(t, ShouldDeleteOutboundTrunkAfterCall("pbx.uiscom.ru:5060"))

	plusofon := outboundProviderProfileForAddress("160907.voice.plusofon.ru:5060")
	require.Equal(t, "plusofon", plusofon.ID)
	require.False(t, plusofon.SkipRegistrationInAuto)
	require.True(t, plusofon.AllowRegisteredInviteDirectFallback)
	require.False(t, plusofon.DeleteTrunkAfterCall)
	require.True(t, plusofon.DefaultG711Only)
	require.Equal(t, outboundProviderQueueScopeTrunk, plusofon.OutboundQueueScope)
	require.Equal(t, 1, plusofon.OutboundMaxConcurrentCalls)

	megapbx := outboundProviderProfileForAddress("company.megapbx.ru:5060")
	require.Equal(t, "megapbx", megapbx.ID)
	require.False(t, megapbx.SkipRegistrationInAuto)
	require.True(t, megapbx.AllowRegisteredInviteDirectFallback)
	require.False(t, megapbx.DeleteTrunkAfterCall)
	require.True(t, megapbx.DefaultG711Only)

	mtt := outboundProviderProfileForAddress("mtt.ru:5060")
	require.Equal(t, "mtt", mtt.ID)
	require.True(t, mtt.AllowRegisteredInviteDirectFallback)
	require.True(t, mtt.DefaultG711Only)

	beeline := outboundProviderProfileForAddress("ip.beeline.ru:5060")
	require.Equal(t, "beeline", beeline.ID)
	require.True(t, beeline.DefaultG711Only)
	require.True(t, beeline.RouteRegisteredInviteToRegistrar)
	require.True(t, beeline.RouteRegistrationToRegistrar)
	require.False(t, beeline.AlwaysRefreshRegistrationBeforeInvite)
	require.Equal(t, 30*time.Second, beeline.MaxRegistrationAgeBeforeInvite)
	require.Equal(t, 3*time.Second, beeline.RegisterInviteSettlingDelay)

	beelineWithTransport := outboundProviderProfileForAddress("ip.beeline.ru:5060;transport=udp")
	require.Equal(t, "beeline", beelineWithTransport.ID)
	require.False(t, beelineWithTransport.AlwaysRefreshRegistrationBeforeInvite)
	require.Equal(t, 30*time.Second, beelineWithTransport.MaxRegistrationAgeBeforeInvite)

	sipuni := outboundProviderProfileForAddress("voip.sipuni.ru:5060")
	require.Equal(t, "sipuni", sipuni.ID)
	require.False(t, sipuni.SkipRegistrationInAuto)
	require.True(t, sipuni.AllowRegisteredInviteDirectFallback)
	require.True(t, sipuni.DeleteTrunkAfterCall)
	require.True(t, sipuni.DefaultG711Only)
	require.Equal(t, outboundProviderQueueScopeProviderFrom, sipuni.OutboundQueueScope)
	require.Equal(t, 1, sipuni.OutboundMaxConcurrentCalls)
	require.True(t, ShouldDeleteOutboundTrunkAfterCall("voip.sipuni.ru:5060"))

	unknown := outboundProviderProfileForAddress("sip.unknown.example:5060")
	require.Equal(t, "universal", unknown.ID)
	require.False(t, unknown.SkipRegistrationInAuto)
	require.True(t, unknown.AllowRegisteredInviteDirectFallback)
	require.False(t, unknown.DeleteTrunkAfterCall)
	require.Equal(t, outboundProviderQueueScopeTrunk, unknown.OutboundQueueScope)
	require.Equal(t, 1, unknown.OutboundMaxConcurrentCalls)

	nearMiss := outboundProviderProfileForAddress("evilnovofon.ru:5060")
	require.Equal(t, "universal", nearMiss.ID)
}

func TestOutboundProviderMediaProfileRestrictsProviderDefaultsToG711(t *testing.T) {
	for _, address := range []string{
		"teleo.telphin.ru:5068",
		"vip1.sip.telphin.com:5060",
		"sip.novofon.ru:5060",
		"pbx.uiscom.ru:5060",
		"login.mtt.ru:5060",
		"voip.sipuni.ru:5060",
		"160907.voice.plusofon.ru:5060",
		"company.megapbx.ru:5060",
	} {
		t.Run(address, func(t *testing.T) {
			reqMedia := &livekit.SIPMediaConfig{}
			mconf, err := newMediaConfig(reqMedia, 0)
			require.NoError(t, err)
			require.True(t, mconf.Codecs.IsEnabledByName(g711.ALawSDPName))
			require.True(t, mconf.Codecs.IsEnabledByName(g711.ULawSDPName))
			require.True(t, mconf.Codecs.IsEnabledByName(g722.SDPName))

			mconf = applyOutboundProviderMediaProfile(address, reqMedia, mconf)
			require.True(t, mconf.Codecs.IsEnabledByName(g711.ALawSDPName))
			require.True(t, mconf.Codecs.IsEnabledByName(g711.ULawSDPName))
			require.False(t, mconf.Codecs.IsEnabledByName(g722.SDPName))
		})
	}
}

func TestOutboundProviderMediaProfileKeepsExplicitCodecs(t *testing.T) {
	for _, address := range []string{
		"teleo.telphin.ru:5068",
		"vip1.sip.telphin.com:5060",
		"sip.novofon.ru:5060",
		"pbx.uiscom.ru:5060",
		"login.mtt.ru:5060",
		"voip.sipuni.ru:5060",
		"160907.voice.plusofon.ru:5060",
		"company.megapbx.ru:5060",
	} {
		t.Run(address, func(t *testing.T) {
			reqMedia := &livekit.SIPMediaConfig{
				Codecs: []*livekit.SIPCodec{{Name: "G722", Rate: 8000}},
			}
			mconf, err := newMediaConfig(reqMedia, 0)
			require.NoError(t, err)

			mconf = applyOutboundProviderMediaProfile(address, reqMedia, mconf)
			require.True(t, mconf.Codecs.IsEnabledByName(g722.SDPName))
		})
	}
}

func TestOutboundConnectErrorClassifiesInviteTimeoutAfterProgress(t *testing.T) {
	call := &outboundCall{
		sigTs: SignalingTimestamps{RingingTime: time.Now()},
	}
	reportErr, status, term, reason := call.classifySIPConnectError(psrpc.NewErrorf(psrpc.Canceled, "sip request timed out"))

	require.NoError(t, reportErr)
	require.Equal(t, callUnavailable, status)
	require.Equal(t, stats.ClientError("request-timeout"), term)
	require.Equal(t, livekit.DisconnectReason_USER_UNAVAILABLE, reason)
}

func TestOutboundConnectErrorClassifiesInviteTimeoutBeforeProgressAsTrunkFailure(t *testing.T) {
	call := &outboundCall{}
	err := psrpc.NewErrorf(psrpc.Canceled, "sip request timed out")
	reportErr, status, term, reason := call.classifySIPConnectError(err)

	require.ErrorIs(t, reportErr, err)
	require.Equal(t, callDropped, status)
	require.Equal(t, stats.ServerError("request-timeout"), term)
	require.Equal(t, livekit.DisconnectReason_SIP_TRUNK_FAILURE, reason)
}

func TestOutboundConnectErrorClassifiesForbiddenAsTrunkFailure(t *testing.T) {
	call := &outboundCall{}
	err := fmt.Errorf("invite failed: %w", &livekit.SIPStatus{
		Code:   livekit.SIPStatusCode_SIP_STATUS_FORBIDDEN,
		Status: "Forbidden",
	})
	reportErr, status, term, reason := call.classifySIPConnectError(err)

	require.ErrorIs(t, reportErr, err)
	require.Equal(t, callDropped, status)
	require.Equal(t, stats.ServerError("forbidden"), term)
	require.Equal(t, livekit.DisconnectReason_SIP_TRUNK_FAILURE, reason)
}

func TestOutboundConnectErrorClassifiesNotFoundAsUserUnavailable(t *testing.T) {
	call := &outboundCall{}
	err := fmt.Errorf("invite failed: %w", &livekit.SIPStatus{
		Code:   livekit.SIPStatusCode_SIP_STATUS_NOTFOUND,
		Status: "Not Found",
	})
	reportErr, status, term, reason := call.classifySIPConnectError(err)

	require.NoError(t, reportErr)
	require.Equal(t, callUnavailable, status)
	require.Equal(t, stats.ClientError("not-found"), term)
	require.Equal(t, livekit.DisconnectReason_USER_UNAVAILABLE, reason)
}

func TestOutboundConnectErrorClassifiesDeclinedAsUserRejected(t *testing.T) {
	call := &outboundCall{}
	err := fmt.Errorf("invite failed: %w", &livekit.SIPStatus{
		Code:   livekit.SIPStatusCode_SIP_STATUS_GLOBAL_DECLINE,
		Status: "Endpoint Not Available",
	})
	reportErr, status, term, reason := call.classifySIPConnectError(err)

	require.NoError(t, reportErr)
	require.Equal(t, callRejected, status)
	require.Equal(t, stats.ClientError("declined"), term)
	require.Equal(t, livekit.DisconnectReason_USER_REJECTED, reason)
}

func TestOutboundConnectErrorClassifiesServerFailuresAsTrunkFailures(t *testing.T) {
	tests := []struct {
		name   string
		code   livekit.SIPStatusCode
		reason string
		want   stats.Termination
	}{
		{name: "internal server error", code: livekit.SIPStatusCode_SIP_STATUS_INTERNAL_SERVER_ERROR, reason: "Server Internal Error", want: stats.ServerError("internal-server-error")},
		{name: "bad gateway", code: livekit.SIPStatusCode_SIP_STATUS_BAD_GATEWAY, reason: "Bad Gateway", want: stats.ServerError("bad-gateway")},
		{name: "service unavailable", code: livekit.SIPStatusCode_SIP_STATUS_SERVICE_UNAVAILABLE, reason: "Service Unavailable", want: stats.ServerError("service-unavailable")},
		{name: "generic 5xx", code: livekit.SIPStatusCode(599), reason: "Server Error", want: stats.ServerError("sip-5xx")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			call := &outboundCall{}
			err := fmt.Errorf("invite failed: %w", &livekit.SIPStatus{
				Code:   tt.code,
				Status: tt.reason,
			})
			reportErr, status, term, reason := call.classifySIPConnectError(err)

			require.ErrorIs(t, reportErr, err)
			require.Equal(t, callDropped, status)
			require.Equal(t, tt.want, term)
			require.Equal(t, livekit.DisconnectReason_SIP_TRUNK_FAILURE, reason)
		})
	}
}

func TestOutboundConnectErrorClassifiesAuthRetryExhaustedAsTrunkFailure(t *testing.T) {
	call := &outboundCall{}
	err := fmt.Errorf("max auth retry attempts reached for SIP invite")
	reportErr, status, term, reason := call.classifySIPConnectError(err)

	require.ErrorIs(t, reportErr, err)
	require.Equal(t, callDropped, status)
	require.Equal(t, stats.ServerError("auth-retry-exhausted"), term)
	require.Equal(t, livekit.DisconnectReason_SIP_TRUNK_FAILURE, reason)
}

func TestClassifyOutboundByeMapsBusyReason(t *testing.T) {
	status, term, reason, sipStatus := classifyOutboundBye(ReasonHeader{
		Type:  "q.850",
		Cause: 17,
		Text:  "User busy",
	})

	require.Equal(t, callRejected, status)
	require.Equal(t, stats.ClientError("busy"), term)
	require.Equal(t, livekit.DisconnectReason_USER_REJECTED, reason)
	require.NotNil(t, sipStatus)
	require.Equal(t, livekit.SIPStatusCode_SIP_STATUS_BUSY_HERE, sipStatus.Code)
	require.Equal(t, "User busy", sipStatus.Status)
}

func TestClassifyOutboundByeKeepsNormalClearingCompleted(t *testing.T) {
	status, term, reason, sipStatus := classifyOutboundBye(ReasonHeader{
		Type:  "q.850",
		Cause: 16,
		Text:  "Normal call clearing",
	})

	require.Equal(t, CallHangup, status)
	require.Equal(t, stats.Success("bye"), term)
	require.Equal(t, livekit.DisconnectReason_CLIENT_INITIATED, reason)
	require.Nil(t, sipStatus)
}

func TestOutboundInviteUsesRegisteredContactUser(t *testing.T) {
	const (
		mockRegisteredUser = "5550101"
		mockCallToUser     = "5550102"
		mockSIPHost        = "sip.mock.example"
	)

	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)
	req := MinimalCreateSIPParticipantRequest()
	req.Address = mockSIPHost + ":5060"
	req.Hostname = ""
	req.Username = mockRegisteredUser
	req.Password = "test-password"
	req.Number = mockRegisteredUser
	req.CallTo = mockCallToUser
	req.WaitUntilAnswered = true
	setOutboundRegisterMode(req, outboundRegisterModeRequired)

	done := make(chan error, 1)
	go func() {
		_, err := client.CreateSIPParticipant(context.Background(), req)
		done <- err
	}()

	registerTx := waitTransaction(t, sipClient)
	require.Equal(t, sip.REGISTER, registerTx.req.Method)
	sendTestResponse(t, registerTx, sip.NewResponseFromRequest(registerTx.req, sip.StatusOK, "OK", nil))

	inviteTx := waitTransaction(t, sipClient)
	require.Equal(t, sip.INVITE, inviteTx.req.Method)
	require.NotNil(t, inviteTx.req.From())
	require.Equal(t, mockRegisteredUser, inviteTx.req.From().Address.User)
	require.Equal(t, mockSIPHost, inviteTx.req.From().Address.Host)
	require.Zero(t, inviteTx.req.From().Address.Port)
	_, hasTransport := inviteTx.req.From().Address.UriParams.Get("transport")
	require.False(t, hasTransport)
	require.NotNil(t, inviteTx.req.Contact())
	require.Equal(t, mockRegisteredUser, inviteTx.req.Contact().Address.User)
	require.NotZero(t, inviteTx.req.Contact().Address.Port)
	userAgent := inviteTx.req.GetHeader("User-Agent")
	require.NotNil(t, userAgent)
	require.Equal(t, UserAgent, userAgent.Value())
	sendTestResponse(t, inviteTx, sip.NewResponseFromRequest(inviteTx.req, sip.StatusForbidden, "Forbidden", nil))

	require.Error(t, <-done)
}

func TestOutboundInviteAutoRetriesWithoutRegistrationOnBusyHere(t *testing.T) {
	const (
		mockRegisteredUser = "5550103"
		mockCallToUser     = "5550106"
		mockSIPHost        = "sip.auto-register.example"
	)

	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)
	req := MinimalCreateSIPParticipantRequest()
	req.Address = mockSIPHost + ":5060"
	req.Hostname = ""
	req.Username = mockRegisteredUser
	req.Password = "test-password"
	req.Number = mockRegisteredUser
	req.CallTo = mockCallToUser
	req.WaitUntilAnswered = true
	setOutboundRegisterMode(req, outboundRegisterModeAuto)

	done := make(chan error, 1)
	go func() {
		_, err := client.CreateSIPParticipant(context.Background(), req)
		done <- err
	}()

	registerTx := waitTransaction(t, sipClient)
	require.Equal(t, sip.REGISTER, registerTx.req.Method)
	sendTestResponse(t, registerTx, sip.NewResponseFromRequest(registerTx.req, sip.StatusOK, "OK", nil))

	registeredInviteTx := waitTransaction(t, sipClient)
	require.Equal(t, sip.INVITE, registeredInviteTx.req.Method)
	registeredCallID := registeredInviteTx.req.CallID().Value()
	require.Equal(t, mockSIPHost, registeredInviteTx.req.From().Address.Host)
	require.Equal(t, mockRegisteredUser, registeredInviteTx.req.Contact().Address.User)
	sendTestResponse(t, registeredInviteTx, sip.NewResponseFromRequest(registeredInviteTx.req, sip.StatusBusyHere, "Busy Here", nil))

	directInviteTx := waitTransaction(t, sipClient)
	require.Equal(t, sip.INVITE, directInviteTx.req.Method)
	require.NotEqual(t, registeredCallID, directInviteTx.req.CallID().Value())
	require.Equal(t, uint32(1), directInviteTx.req.CSeq().SeqNo)
	require.Equal(t, client.sconf.SignalingIP.String(), directInviteTx.req.From().Address.Host)
	require.NotZero(t, directInviteTx.req.From().Address.Port)
	require.Empty(t, directInviteTx.req.Contact().Address.User)
	sendTestResponse(t, directInviteTx, sip.NewResponseFromRequest(directInviteTx.req, sip.StatusBusyHere, "Busy Here", nil))

	require.Error(t, <-done)
}

func TestOutboundInviteUiscomAllowsDirectFallbackAfterRegisteredBusyHere(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)
	req := MinimalCreateSIPParticipantRequest()
	req.Address = "pbx.uiscom.ru:5060"
	req.Hostname = ""
	req.Username = "0526470"
	req.Password = "test-password"
	req.Number = "0526470"
	req.CallTo = "+77057756019"
	req.WaitUntilAnswered = true
	setOutboundRegisterMode(req, outboundRegisterModeAuto)

	done := make(chan error, 1)
	go func() {
		_, err := client.CreateSIPParticipant(context.Background(), req)
		done <- err
	}()

	registerTx := waitTransaction(t, sipClient)
	require.Equal(t, sip.REGISTER, registerTx.req.Method)
	sendTestResponse(t, registerTx, sip.NewResponseFromRequest(registerTx.req, sip.StatusOK, "OK", nil))

	registeredInviteTx := waitTransaction(t, sipClient)
	require.Equal(t, sip.INVITE, registeredInviteTx.req.Method)
	registeredCallID := registeredInviteTx.req.CallID().Value()
	sendTestResponse(t, registeredInviteTx, sip.NewResponseFromRequest(registeredInviteTx.req, sip.StatusBusyHere, "Busy Here", nil))

	directInviteTx := waitTransaction(t, sipClient)
	require.Equal(t, sip.INVITE, directInviteTx.req.Method)
	require.NotEqual(t, registeredCallID, directInviteTx.req.CallID().Value())
	require.Equal(t, uint32(1), directInviteTx.req.CSeq().SeqNo)
	require.Equal(t, client.sconf.SignalingIP.String(), directInviteTx.req.From().Address.Host)
	require.NotZero(t, directInviteTx.req.From().Address.Port)
	require.Empty(t, directInviteTx.req.Contact().Address.User)
	sendTestResponse(t, directInviteTx, sip.NewResponseFromRequest(directInviteTx.req, sip.StatusBusyHere, "Busy Here", nil))
	require.Error(t, <-done)
}

func TestOutboundInviteWaitsBrieflyAfterFreshRegister(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)
	req := MinimalCreateSIPParticipantRequest()
	req.Address = "pbx.uiscom.ru:5060"
	req.Hostname = ""
	req.Username = "0526470"
	req.Password = "test-password"
	req.Number = "0526470"
	req.CallTo = "+77057756019"
	req.WaitUntilAnswered = true

	prevDelay := invitePostRegisterSettlingDelay
	invitePostRegisterSettlingDelay = 50 * time.Millisecond
	defer func() {
		invitePostRegisterSettlingDelay = prevDelay
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := client.CreateSIPParticipant(ctx, req)
		done <- err
	}()

	registerTx := waitTransaction(t, sipClient)
	require.Equal(t, sip.REGISTER, registerTx.req.Method)
	sendTestResponse(t, registerTx, sip.NewResponseFromRequest(registerTx.req, sip.StatusOK, "OK", nil))

	select {
	case tx := <-sipClient.transactions:
		t.Fatalf("unexpected %s before post-register settling delay elapsed", tx.req.Method)
	case <-time.After(20 * time.Millisecond):
	}

	inviteTx := waitTransactionWithTimeout(t, sipClient, 200*time.Millisecond)
	require.Equal(t, sip.INVITE, inviteTx.req.Method)
	cancel()
	require.Error(t, <-done)
}

func TestRetryInviteAfterFreshRegisterForceReregistersOnTemporaryFailures(t *testing.T) {
	tests := []struct {
		name   string
		status sip.StatusCode
		reason string
		body   []byte
	}{
		{name: "temporarily unavailable", status: sip.StatusTemporarilyUnavailable, reason: "Temporarily Unavailable"},
		{name: "service unavailable", status: sip.StatusServiceUnavailable, reason: "Service Unavailable"},
		{
			name:   "forbidden ims initial registration",
			status: sip.StatusForbidden,
			reason: "Forbidden",
			body:   []byte(`<?xml version="1.0" encoding="UTF-8"?><ims-3gpp version="1"><alternative-service><type>restoration</type><reason>timeout</reason><action>initial-registration</action></alternative-service></ims-3gpp>`),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := NewOutboundTestClient(t, TestClientConfig{})
			sipClient := getCreatedSIPClient(t)

			conf, err := resolveRegistrationConfig(sipOutboundConfig{
				address: "sip.retry-register.example:5060",
				host:    "sip.retry-register.example",
				user:    "5550107",
				pass:    "test-password",
			})
			require.NoError(t, err)
			client.registrationManager.markSuccessfulRegister(conf.cacheKey(), time.Now())

			outbound := &sipOutbound{
				c:   client,
				log: logger.GetLogger(),
			}
			req := sip.NewRequest(sip.INVITE, sip.Uri{Host: "sip.retry-register.example"})
			resp := sip.NewResponseFromRequest(req, tc.status, tc.reason, nil)
			if len(tc.body) != 0 {
				resp.SetBody(tc.body)
			}

			type result struct {
				retried bool
				err     error
			}
			done := make(chan result, 1)
			go func() {
				retried, err := outbound.retryInviteAfterFreshRegister(context.Background(), conf, "test-password", resp)
				done <- result{retried: retried, err: err}
			}()

			registerTx := waitTransactionWithTimeout(t, sipClient, 3*time.Second)
			require.Equal(t, sip.REGISTER, registerTx.req.Method)
			sendTestResponse(t, registerTx, sip.NewResponseFromRequest(registerTx.req, sip.StatusOK, "OK", nil))

			res := <-done
			require.NoError(t, res.err)
			require.True(t, res.retried)
		})
	}
}

func TestRetryInviteAfterFreshRegisterDoesNotReregisterOnGenericForbidden(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})

	conf, err := resolveRegistrationConfig(sipOutboundConfig{
		address: "sip.retry-register.example:5060",
		host:    "sip.retry-register.example",
		user:    "5550107",
		pass:    "test-password",
	})
	require.NoError(t, err)
	client.registrationManager.markSuccessfulRegister(conf.cacheKey(), time.Now())

	outbound := &sipOutbound{
		c:   client,
		log: logger.GetLogger(),
	}
	req := sip.NewRequest(sip.INVITE, sip.Uri{Host: "sip.retry-register.example"})
	resp := sip.NewResponseFromRequest(req, sip.StatusForbidden, "Forbidden", nil)

	retried, err := outbound.retryInviteAfterFreshRegister(context.Background(), conf, "test-password", resp)
	require.NoError(t, err)
	require.False(t, retried)
}

func TestRetryInviteAfterServiceNotAuthorisedReregistersAndUsesServiceRoute(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)

	conf, err := resolveRegistrationConfig(sipOutboundConfig{
		address: "ip.beeline.ru:5060",
		host:    "ip.beeline.ru",
		user:    "9063671384",
		pass:    "test-password",
	})
	require.NoError(t, err)

	outbound := &sipOutbound{
		c:                                client,
		log:                              logger.GetLogger(),
		configuredRouteHeaders:           []string{"<sip:configured-proxy.example.com;lr>"},
		routeHeaders:                     []string{"<sip:configured-proxy.example.com;lr>", "<sip:ip.beeline.ru:5060;transport=udp;lr>"},
		routeRegisteredInviteToRegistrar: true,
	}
	req := sip.NewRequest(sip.INVITE, sip.Uri{Host: "ip.beeline.ru"})
	resp := sip.NewResponseFromRequest(req, sip.StatusForbidden, "Forbidden", nil)
	resp.AppendHeader(sip.NewHeader("Warning", `127 invaild.com "Service not authorised"`))

	type result struct {
		retried bool
		err     error
	}
	done := make(chan result, 1)
	go func() {
		retried, err := outbound.retryInviteAfterFreshRegister(context.Background(), conf, "test-password", resp)
		done <- result{retried: retried, err: err}
	}()

	registerTx := waitTransactionWithTimeout(t, sipClient, 3*time.Second)
	require.Equal(t, sip.REGISTER, registerTx.req.Method)
	ok := sip.NewResponseFromRequest(registerTx.req, sip.StatusOK, "OK", nil)
	ok.AppendHeader(sip.NewHeader("Service-Route", "<sip:212.119.246.230:5060;transport=udp;lr;mpcftk=1-115-30c-8-4006a2a2>"))
	sendTestResponse(t, registerTx, ok)

	res := <-done
	require.NoError(t, res.err)
	require.True(t, res.retried)
	require.Equal(t, []string{"<sip:212.119.246.230:5060;transport=udp;lr;mpcftk=1-115-30c-8-4006a2a2>"}, conf.ServiceRouteHeaders)
	require.Equal(t, []string{
		"<sip:configured-proxy.example.com;lr>",
		"<sip:212.119.246.230:5060;transport=udp;lr;mpcftk=1-115-30c-8-4006a2a2>",
	}, outbound.routeHeaders)
}

func TestRegisteredInviteRouteHeadersPreferServiceRouteOverRegistrarFallback(t *testing.T) {
	conf, err := resolveRegistrationConfig(sipOutboundConfig{
		address: "ip.beeline.ru:5060",
		host:    "ip.beeline.ru",
		user:    "9063671384",
		pass:    "test-password",
	})
	require.NoError(t, err)
	conf.ServiceRouteHeaders = []string{"<sip:212.119.246.230:5060;transport=udp;lr;mpcftk=1-115-30c-8-4006a2a2>"}

	require.Equal(t, []string{
		"<sip:configured-proxy.example.com;lr>",
		"<sip:212.119.246.230:5060;transport=udp;lr;mpcftk=1-115-30c-8-4006a2a2>",
	}, registeredInviteRouteHeaders([]string{"<sip:configured-proxy.example.com;lr>"}, conf, true))
}

func TestOutboundInviteDigestURIUsesRequestURIWithPort(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)
	outbound := newTestOutboundCall(client)
	toURI := CreateURIFromUserAndAddress("+79998887722", "login.test.com:5060", TransportUDP)

	result := make(chan error, 1)
	go func() {
		_, err := outbound.cc.Invite(context.Background(), toURI, nil, "123456", "test-password", nil, []byte("v=0\r\n"), nil, nil)
		result <- err
	}()

	firstInvite := waitTransaction(t, sipClient)
	require.Equal(t, sip.INVITE, firstInvite.req.Method)
	require.Equal(t, "sip:+79998887722@login.test.com:5060;transport=udp", firstInvite.req.Recipient.String())

	challenge := digest.Challenge{
		Realm:     "login.test.com",
		Nonce:     "12345678901234567890123456789012",
		Algorithm: "MD5",
		QOP:       []string{"auth"},
	}
	resp := sip.NewResponseFromRequest(firstInvite.req, sip.StatusUnauthorized, "Unauthorized", nil)
	resp.RemoveHeader("To")
	resp.AppendHeader(&sip.ToHeader{Address: sip.Uri{
		User: "+79998887722",
		Host: "login.test.com",
		UriParams: sip.HeaderParams{{
			K: "transport",
			V: "udp",
		}},
	}})
	resp.AppendHeader(sip.NewHeader("WWW-Authenticate", challenge.String()))
	sendTestResponse(t, firstInvite, resp)

	authInvite := waitTransaction(t, sipClient)
	require.Equal(t, sip.INVITE, authInvite.req.Method)
	authHeader := authInvite.req.GetHeader("Authorization")
	require.NotNil(t, authHeader)
	cred, err := digest.ParseCredentials(authHeader.Value())
	require.NoError(t, err)
	require.Equal(t, firstInvite.req.Recipient.String(), cred.URI)
	require.Equal(t, "sip:+79998887722@login.test.com:5060;transport=udp", cred.URI)

	sendTestResponse(t, authInvite, sip.NewResponseFromRequest(authInvite.req, sip.StatusOK, "OK", []byte("v=0\r\n")))
	require.NoError(t, <-result)
}

func TestOutboundInviteKeepsProxyAuthorizationWhenWWWAuthenticateFollows(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)
	outbound := newTestOutboundCall(client)
	toURI := CreateURIFromUserAndAddress("+79998887722", "proxy.auth.example:5060", TransportUDP)

	result := make(chan error, 1)
	go func() {
		_, err := outbound.cc.Invite(context.Background(), toURI, nil, "123456", "test-password", nil, []byte("v=0\r\n"), nil, nil)
		result <- err
	}()

	firstInvite := waitTransaction(t, sipClient)
	require.Equal(t, sip.INVITE, firstInvite.req.Method)
	require.Nil(t, firstInvite.req.GetHeader("Proxy-Authorization"))
	sendProxyAuthRequired(t, firstInvite)

	proxyAuthInvite := waitTransaction(t, sipClient)
	require.Equal(t, sip.INVITE, proxyAuthInvite.req.Method)
	require.NotNil(t, proxyAuthInvite.req.GetHeader("Proxy-Authorization"))
	require.Nil(t, proxyAuthInvite.req.GetHeader("Authorization"))

	challenge := digest.Challenge{
		Realm:     "proxy.auth.example",
		Nonce:     "nonce-www-authenticate",
		Algorithm: "MD5",
		QOP:       []string{"auth"},
	}
	unauthorized := sip.NewResponseFromRequest(proxyAuthInvite.req, sip.StatusUnauthorized, "Unauthorized", nil)
	unauthorized.AppendHeader(sip.NewHeader("WWW-Authenticate", challenge.String()))
	sendTestResponse(t, proxyAuthInvite, unauthorized)

	bothAuthInvite := waitTransaction(t, sipClient)
	require.Equal(t, sip.INVITE, bothAuthInvite.req.Method)
	require.NotNil(t, bothAuthInvite.req.GetHeader("Proxy-Authorization"))
	require.NotNil(t, bothAuthInvite.req.GetHeader("Authorization"))

	sendTestResponse(t, bothAuthInvite, sip.NewResponseFromRequest(bothAuthInvite.req, sip.StatusOK, "OK", []byte("v=0\r\n")))
	require.NoError(t, <-result)
}

func TestOutboundInviteSentCallbackRunsBeforeFinalResponse(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)
	outbound := newTestOutboundCall(client)
	toURI := CreateURIFromUserAndAddress("+79998887722", "login.test.com:5060", TransportUDP)

	inviteSent := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		_, err := outbound.cc.Invite(context.Background(), toURI, nil, "", "", nil, []byte("v=0\r\n"), nil, func() {
			close(inviteSent)
		})
		result <- err
	}()

	invite := waitTransaction(t, sipClient)
	require.Equal(t, sip.INVITE, invite.req.Method)
	select {
	case <-inviteSent:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected invite sent callback before final SIP response")
	}

	sendTestResponse(t, invite, sip.NewResponseFromRequest(invite.req, sip.StatusBusyHere, "Busy Here", nil))
	require.Error(t, <-result)
}

func TestOutboundInviteConfiguredRouteHeadersPreserveOrder(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)
	outbound := newTestOutboundCall(client)
	outbound.cc.routeHeaders = []string{
		"<sip:edge-1.example.com;lr>",
		"<sip:edge-2.example.com;lr>",
	}
	toURI := CreateURIFromUserAndAddress("+79998887722", "login.test.com:5060", TransportUDP)

	result := make(chan error, 1)
	go func() {
		_, err := outbound.cc.Invite(context.Background(), toURI, nil, "", "", nil, []byte("v=0\r\n"), nil, nil)
		result <- err
	}()

	invite := waitTransaction(t, sipClient)
	require.Equal(t, sip.INVITE, invite.req.Method)
	routeHeaders := invite.req.GetHeaders("Route")
	require.Len(t, routeHeaders, 2)
	require.Equal(t, "<sip:edge-1.example.com;lr>", routeHeaders[0].Value())
	require.Equal(t, "<sip:edge-2.example.com;lr>", routeHeaders[1].Value())

	sendTestResponse(t, invite, sip.NewResponseFromRequest(invite.req, sip.StatusBusyHere, "Busy Here", nil))
	require.Error(t, <-result)
}

func TestOutboundInviteRequestTerminatedIsClientError(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)
	sessionEnd := make(chan string, 1)
	client.SetHandler(&TestHandler{
		OnSessionEndFunc: func(ctx context.Context, callIdentifier *CallIdentifier, callInfo *livekit.SIPCallInfo, reason string) {
			sessionEnd <- reason
		},
	})
	req := MinimalCreateSIPParticipantRequest()
	req.Address = "pbx.uiscom.ru:5060"
	req.Hostname = ""
	req.Username = "0526470"
	req.Password = "test-password"
	req.Number = "0526470"
	req.CallTo = "+77057756019"
	req.WaitUntilAnswered = true
	setOutboundRegisterMode(req, outboundRegisterModeDisabled)

	done := make(chan error, 1)
	go func() {
		_, err := client.CreateSIPParticipant(context.Background(), req)
		done <- err
	}()

	inviteTx := waitTransaction(t, sipClient)
	require.Equal(t, sip.INVITE, inviteTx.req.Method)
	sendTestResponse(t, inviteTx, sip.NewResponseFromRequest(inviteTx.req, sip.StatusRequestTerminated, "Request Terminated", nil))

	err := <-done
	require.Error(t, err)
	var sipErr *livekit.SIPStatus
	require.ErrorAs(t, err, &sipErr)
	require.Equal(t, livekit.SIPStatusCode_SIP_STATUS_REQUEST_TERMINATED, sipErr.Code)

	select {
	case reason := <-sessionEnd:
		require.Equal(t, "request-terminated", reason)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for OnSessionEnd")
	}
}

func TestOutboundInviteRequestTimeoutIsClientError(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)
	sessionEnd := make(chan string, 1)
	client.SetHandler(&TestHandler{
		OnSessionEndFunc: func(ctx context.Context, callIdentifier *CallIdentifier, callInfo *livekit.SIPCallInfo, reason string) {
			sessionEnd <- reason
		},
	})
	req := MinimalCreateSIPParticipantRequest()
	req.Address = "pbx.uiscom.ru:5060"
	req.Hostname = ""
	req.Username = "0526470"
	req.Password = "test-password"
	req.Number = "0526470"
	req.CallTo = "+77057756019"
	req.WaitUntilAnswered = true
	setOutboundRegisterMode(req, outboundRegisterModeDisabled)

	done := make(chan error, 1)
	go func() {
		_, err := client.CreateSIPParticipant(context.Background(), req)
		done <- err
	}()

	inviteTx := waitTransaction(t, sipClient)
	require.Equal(t, sip.INVITE, inviteTx.req.Method)
	sendTestResponse(t, inviteTx, sip.NewResponseFromRequest(inviteTx.req, sip.StatusRequestTimeout, "Request Timeout", nil))

	err := <-done
	require.Error(t, err)
	var sipErr *livekit.SIPStatus
	require.ErrorAs(t, err, &sipErr)
	require.Equal(t, livekit.SIPStatusCode_SIP_STATUS_REQUEST_TIMEOUT, sipErr.Code)

	select {
	case reason := <-sessionEnd:
		require.Equal(t, "request-timeout", reason)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for OnSessionEnd")
	}
}

func TestOutboundInviteSkipsRegisterWhenDisabled(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)
	req := MinimalCreateSIPParticipantRequest()
	req.Address = "sip.no-register.example:5060"
	req.Hostname = ""
	req.Username = "5550104"
	req.Password = "test-password"
	req.Number = "5550104"
	req.WaitUntilAnswered = true
	setOutboundRegisterMode(req, outboundRegisterModeDisabled)

	done := make(chan error, 1)
	go func() {
		_, err := client.CreateSIPParticipant(context.Background(), req)
		done <- err
	}()

	inviteTx := waitTransaction(t, sipClient)
	require.Equal(t, sip.INVITE, inviteTx.req.Method)
	require.NotNil(t, inviteTx.req.From())
	require.Equal(t, "5550104", inviteTx.req.From().Address.User)
	require.Equal(t, client.sconf.SignalingIP.String(), inviteTx.req.From().Address.Host)
	require.NotZero(t, inviteTx.req.From().Address.Port)
	_, hasTransport := inviteTx.req.From().Address.UriParams.Get("transport")
	require.True(t, hasTransport)
	require.NotNil(t, inviteTx.req.Contact())
	require.Empty(t, inviteTx.req.Contact().Address.User)
	sendTestResponse(t, inviteTx, sip.NewResponseFromRequest(inviteTx.req, sip.StatusForbidden, "Forbidden", nil))

	require.Error(t, <-done)
}

func TestOutboundInviteAutoSkipsRegisterForConfiguredHost(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)

	req := MinimalCreateSIPParticipantRequest()
	req.Address = "proxy.sip.novofon.ru:5060"
	req.Hostname = ""
	req.Username = "5550104"
	req.Password = "test-password"
	req.Number = "+74951234567"
	req.WaitUntilAnswered = true
	setOutboundRegisterMode(req, outboundRegisterModeAuto)

	done := make(chan error, 1)
	go func() {
		_, err := client.CreateSIPParticipant(context.Background(), req)
		done <- err
	}()

	inviteTx := waitTransaction(t, sipClient)
	require.Equal(t, sip.INVITE, inviteTx.req.Method)
	sendTestResponse(t, inviteTx, sip.NewResponseFromRequest(inviteTx.req, sip.StatusForbidden, "Forbidden", nil))
	require.Error(t, <-done)
}

func TestOutboundInviteAutoSkipsRegisterForNovofon(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)
	req := MinimalCreateSIPParticipantRequest()
	req.Address = "sip.novofon.ru:5060"
	req.Hostname = ""
	req.Username = "0101536"
	req.Password = "test-password"
	req.Number = "0101536"
	req.WaitUntilAnswered = true
	setOutboundRegisterMode(req, outboundRegisterModeAuto)

	done := make(chan error, 1)
	go func() {
		_, err := client.CreateSIPParticipant(context.Background(), req)
		done <- err
	}()

	inviteTx := waitTransaction(t, sipClient)
	require.Equal(t, sip.INVITE, inviteTx.req.Method)
	sendTestResponse(t, inviteTx, sip.NewResponseFromRequest(inviteTx.req, sip.StatusForbidden, "Forbidden", nil))
	require.Error(t, <-done)
}

func TestOutboundInviteRegisterSkippedIPTrunk407ReturnsProviderAuthErrorAfterRetries(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)

	req := MinimalCreateSIPParticipantRequest()
	req.Address = "proxy.sip.novofon.ru:5060"
	req.Hostname = ""
	req.Username = "0101536"
	req.Password = "test-password"
	req.Number = "0101536"
	req.WaitUntilAnswered = true
	setOutboundRegisterMode(req, outboundRegisterModeAuto)

	done := make(chan error, 1)
	go func() {
		_, err := client.CreateSIPParticipant(context.Background(), req)
		done <- err
	}()

	inviteTx := waitTransaction(t, sipClient)
	require.Equal(t, sip.INVITE, inviteTx.req.Method)
	require.Nil(t, inviteTx.req.GetHeader("Proxy-Authorization"))
	sendProxyAuthRequired(t, inviteTx)

	authInviteTx := waitTransaction(t, sipClient)
	require.Equal(t, sip.INVITE, authInviteTx.req.Method)
	require.NotNil(t, authInviteTx.req.GetHeader("Proxy-Authorization"))
	sendProxyAuthRequired(t, authInviteTx)

	finalAuthInviteTx := waitTransaction(t, sipClient)
	require.Equal(t, sip.INVITE, finalAuthInviteTx.req.Method)
	require.NotNil(t, finalAuthInviteTx.req.GetHeader("Proxy-Authorization"))
	sendProxyAuthRequired(t, finalAuthInviteTx)

	err := <-done
	require.Error(t, err)
	var providerErr providerAuthConfigError
	require.ErrorAs(t, err, &providerErr)
	require.Equal(t, sip.StatusProxyAuthRequired, providerErr.status)

	select {
	case retryTx := <-sipClient.transactions:
		t.Cleanup(func() { retryTx.transaction.Terminate() })
		t.Fatalf("unexpected retry transaction: %s", retryTx.req.Method)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestOutboundInviteRequiresRegisterWhenConfigured(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)
	req := MinimalCreateSIPParticipantRequest()
	req.Address = "sip.requires-register.example:5060"
	req.Hostname = ""
	req.Username = "5550105"
	req.Password = "test-password"
	req.Number = "5550105"
	req.WaitUntilAnswered = true
	setOutboundRegisterMode(req, outboundRegisterModeRequired)

	done := make(chan error, 1)
	go func() {
		_, err := client.CreateSIPParticipant(context.Background(), req)
		done <- err
	}()

	registerTx := waitTransaction(t, sipClient)
	require.Equal(t, sip.REGISTER, registerTx.req.Method)
	sendTestResponse(t, registerTx, sip.NewResponseFromRequest(registerTx.req, sip.StatusForbidden, "Forbidden", nil))
	require.Error(t, <-done)
}

func TestOutboundInviteRequiredOverridesProviderProfileSkipRegister(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)
	req := MinimalCreateSIPParticipantRequest()
	req.Address = "sip.novofon.ru:5060"
	req.Hostname = ""
	req.Username = "0101536"
	req.Password = "test-password"
	req.Number = "0101536"
	req.WaitUntilAnswered = true
	setOutboundRegisterMode(req, outboundRegisterModeRequired)

	done := make(chan error, 1)
	go func() {
		_, err := client.CreateSIPParticipant(context.Background(), req)
		done <- err
	}()

	registerTx := waitTransaction(t, sipClient)
	require.Equal(t, sip.REGISTER, registerTx.req.Method)
	sendTestResponse(t, registerTx, sip.NewResponseFromRequest(registerTx.req, sip.StatusForbidden, "Forbidden", nil))
	require.Error(t, <-done)
}

func TestOutboundInviteAutoFallsBackToDirectInviteAfterUniversalRegisterFailure(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)
	req := MinimalCreateSIPParticipantRequest()
	req.Address = "sip.universal.example:5060"
	req.Hostname = ""
	req.Username = "5550106"
	req.Password = "test-password"
	req.Number = "5550106"
	req.WaitUntilAnswered = true
	setOutboundRegisterMode(req, outboundRegisterModeAuto)

	done := make(chan error, 1)
	go func() {
		_, err := client.CreateSIPParticipant(context.Background(), req)
		done <- err
	}()

	registerTx := waitTransaction(t, sipClient)
	require.Equal(t, sip.REGISTER, registerTx.req.Method)
	sendTestResponse(t, registerTx, sip.NewResponseFromRequest(registerTx.req, sip.StatusForbidden, "Forbidden", nil))

	inviteTx := waitTransaction(t, sipClient)
	require.Equal(t, sip.INVITE, inviteTx.req.Method)
	require.NotNil(t, inviteTx.req.From())
	require.Equal(t, client.sconf.SignalingIP.String(), inviteTx.req.From().Address.Host)
	require.NotNil(t, inviteTx.req.Contact())
	require.Empty(t, inviteTx.req.Contact().Address.User)
	sendTestResponse(t, inviteTx, sip.NewResponseFromRequest(inviteTx.req, sip.StatusForbidden, "Forbidden", nil))
	require.Error(t, <-done)
}

func TestOutboundInviteWithoutRegistrationKeepsHostOnlyContact(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)
	req := MinimalCreateSIPParticipantRequest()
	req.WaitUntilAnswered = true
	setOutboundRegisterMode(req, outboundRegisterModeDisabled)

	done := make(chan error, 1)
	go func() {
		_, err := client.CreateSIPParticipant(context.Background(), req)
		done <- err
	}()

	inviteTx := waitTransaction(t, sipClient)
	require.Equal(t, sip.INVITE, inviteTx.req.Method)
	require.NotNil(t, inviteTx.req.Contact())
	require.Empty(t, inviteTx.req.Contact().Address.User)
	userAgent := inviteTx.req.GetHeader("User-Agent")
	require.NotNil(t, userAgent)
	require.Equal(t, UserAgent, userAgent.Value())
	sendTestResponse(t, inviteTx, sip.NewResponseFromRequest(inviteTx.req, sip.StatusForbidden, "Forbidden", nil))

	require.Error(t, <-done)
}

func TestInviteRequestLogFieldsRedactsAuthorization(t *testing.T) {
	const (
		mockRegisteredUser = "5550101"
		mockCallToUser     = "5550102"
		mockSIPHost        = "sip.mock.example"
		mockLocalIP        = "192.0.2.10"
		mockLocalAddr      = mockLocalIP + ":15060"
		mockDestAddr       = "198.51.100.10:5060"
	)

	req := sip.NewRequest(sip.INVITE, sip.Uri{
		User: mockCallToUser,
		Host: mockSIPHost,
		UriParams: sip.HeaderParams{
			{K: "transport", V: "udp"},
		},
	})
	req.SetSource(mockLocalAddr)
	req.SetDestination(mockDestAddr)
	req.AppendHeader(&sip.FromHeader{
		DisplayName: mockRegisteredUser,
		Address:     sip.Uri{User: mockRegisteredUser, Host: mockSIPHost},
	})
	req.AppendHeader(&sip.ToHeader{Address: sip.Uri{User: mockCallToUser, Host: mockSIPHost}})
	req.AppendHeader(&sip.ContactHeader{Address: sip.Uri{User: mockRegisteredUser, Host: mockLocalIP, Port: 15060}})
	req.AppendHeader(&sip.ViaHeader{
		ProtocolName:    "SIP",
		ProtocolVersion: "2.0",
		Transport:       "UDP",
		Host:            mockLocalIP,
		Port:            15060,
	})
	req.AppendHeader(sip.NewHeader("Call-ID", "call-id"))
	req.AppendHeader(&sip.CSeqHeader{SeqNo: 2, MethodName: sip.INVITE})
	req.AppendHeader(sip.NewHeader("Content-Type", "application/sdp"))
	req.AppendHeader(sip.NewHeader("User-Agent", UserAgent))
	req.AppendHeader(sip.NewHeader("Route", "<sip:proxy.example.com;lr>"))
	req.AppendHeader(sip.NewHeader("Proxy-Authorization", `Digest username="5550101", response="mock-digest-response"`))
	req.SetBody([]byte("v=0\r\n"))

	fields := logFieldsMap(inviteRequestLogFields(req))
	require.Equal(t, "sip:5550102@sip.mock.example;transport=udp", fields["request_uri"])
	require.Equal(t, mockDestAddr, fields["dest_addr"])
	require.Equal(t, mockLocalAddr, fields["local_addr"])
	require.Equal(t, "sip:5550101@sip.mock.example", fields["from_uri"])
	require.Equal(t, mockRegisteredUser, fields["from_display_name"])
	require.Equal(t, "sip:5550101@192.0.2.10:15060", fields["contact_uri"])
	require.Equal(t, mockLocalAddr, fields["via_sent_by"])
	require.Equal(t, uint32(2), fields["cseq"])
	require.Equal(t, "application/sdp", fields["content_type"])
	require.Equal(t, UserAgent, fields["user_agent"])
	require.Equal(t, false, fields["has_authorization"])
	require.Equal(t, true, fields["has_proxy_authorization"])
	require.Equal(t, []string{"<sip:proxy.example.com;lr>"}, fields["route_headers"])
	headers := fields["headers"].(map[string][]string)
	require.Equal(t, []string{`"5550101" <sip:5550101@sip.mock.example>`}, headers["From"])
	require.Equal(t, []string{"<sip:proxy.example.com;lr>"}, headers["Route"])
	require.NotContains(t, headers, "Proxy-Authorization")

	rendered := fmt.Sprint(inviteRequestLogFields(req))
	require.NotContains(t, rendered, "mock-digest-response")
	require.NotContains(t, rendered, "Proxy-Authorization")
}

func TestInviteAuthLogFieldsAreSanitized(t *testing.T) {
	const (
		rawNonce       = "raw-provider-nonce"
		rawResponse    = "raw-digest-response"
		rawPassword    = "secret-password"
		rawCnonce      = "raw-client-nonce"
		authHeaderName = "Proxy-Authenticate"
		respHeaderName = "Proxy-Authorization"
	)

	challenge := &digest.Challenge{
		Realm:     "novofon",
		Nonce:     rawNonce,
		Opaque:    "opaque-value",
		Algorithm: "MD5",
		QOP:       []string{"auth"},
		Domain:    []string{"sip:sip.novofon.ru"},
	}
	req := sip.NewRequest(sip.INVITE, sip.Uri{
		User: "+79993441100",
		Host: "sip.novofon.ru",
	})
	cred := &digest.Credentials{
		Username:  "0101536",
		URI:       "sip:+79993441100@sip.novofon.ru",
		Cnonce:    rawCnonce,
		Nc:        1,
		Realm:     "novofon",
		Nonce:     rawNonce,
		Algorithm: "MD5",
		QOP:       "auth",
		Response:  rawResponse,
	}

	challengeFields := inviteAuthChallengeLogFields(sip.StatusProxyAuthRequired, authHeaderName, respHeaderName, challenge)
	responseFields := inviteAuthResponseLogFields(respHeaderName, req, cred, cred.URI)
	allFields := append(challengeFields, responseFields...)
	challengeLog := logFieldsMap(challengeFields)
	responseLog := logFieldsMap(responseFields)

	require.Equal(t, sip.StatusProxyAuthRequired, challengeLog["status"])
	require.Equal(t, authHeaderName, challengeLog["auth_challenge_header"])
	require.Equal(t, respHeaderName, challengeLog["auth_response_header"])
	require.Equal(t, "novofon", challengeLog["realm"])
	require.Equal(t, "MD5", challengeLog["algorithm"])
	require.Equal(t, []string{"auth"}, challengeLog["qop"])
	require.Equal(t, true, challengeLog["opaque_present"])
	require.Equal(t, len(rawNonce), challengeLog["nonce_len"])
	require.Equal(t, shortSHA256Hex(rawNonce), challengeLog["nonce_hash"])
	require.Equal(t, "0101536", responseLog["username"])
	require.Equal(t, cred.URI, responseLog["digest_uri"])
	require.Equal(t, req.Recipient.String(), responseLog["request_uri"])
	require.Equal(t, true, responseLog["has_proxy_authorization"])

	rendered := fmt.Sprint(allFields)
	require.NotContains(t, rendered, rawNonce)
	require.NotContains(t, rendered, rawResponse)
	require.NotContains(t, rendered, rawPassword)
	require.NotContains(t, rendered, rawCnonce)
	require.NotContains(t, rendered, "opaque-value")
}

func logFieldsMap(fields []interface{}) map[interface{}]interface{} {
	out := make(map[interface{}]interface{}, len(fields)/2)
	for i := 0; i+1 < len(fields); i += 2 {
		out[fields[i]] = fields[i+1]
	}
	return out
}

func TestOutboundRouteHeaderWithRecordRoute(t *testing.T) {
	// Make sure the ACK doesn't carry over initial Route header.
	// Steps:
	// 1. Create a SIP participant with an initial Route header.
	// 2. Make sure the Route header is properly populates in INVITE.
	// 3. Fake a 200 response with Record Route headers.
	// 4. Make sure the ACK doesn't carry over initial Route header..

	// Plumbing
	initialRouteURI := sip.Uri{Host: "initial-header.com", UriParams: sip.HeaderParams{{"lr", ""}}}
	addedRouteURI := sip.Uri{Host: "added-header.com", UriParams: sip.HeaderParams{{"lr", ""}}}
	initialRouteHeader := sip.RouteHeader{Address: initialRouteURI}
	addedRouteHeader := sip.RouteHeader{Address: addedRouteURI}
	client := NewOutboundTestClient(t, TestClientConfig{})
	req := MinimalCreateSIPParticipantRequest()
	req.Headers = map[string]string{
		"Route": initialRouteHeader.Value(),
	}
	setOutboundRegisterMode(req, outboundRegisterModeDisabled)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { // Allow test to continue
		_, err := client.CreateSIPParticipant(ctx, req)
		if err != nil && ctx.Err() == nil {
			// Only log error if context wasn't cancelled
			t.Logf("CreateSIPParticipant error: %v", err)
		}
	}()

	t.Log("Waiting for INVITE to be sent")

	var sipClient *testSIPClient
	select {
	case sipClient = <-createdClients:
		t.Cleanup(func() { _ = sipClient.Close() })
	case <-time.After(100 * time.Millisecond):
		cancel()
		require.Fail(t, "expected client to be created")
		return
	}

	var tr *transactionRequest
	select {
	case tr = <-sipClient.transactions:
		t.Cleanup(func() { tr.transaction.Terminate() })
	case <-time.After(500 * time.Millisecond):
		cancel()
		require.Fail(t, "expected transaction request to be created")
		return
	}

	fmt.Println("Received INVITE, validating")

	require.NotNil(t, tr)
	require.NotNil(t, tr.req)
	require.NotNil(t, tr.transaction)
	require.Equal(t, sip.INVITE, tr.req.Method)
	routeHeaders := tr.req.GetHeaders("Route")
	require.Equal(t, 1, len(routeHeaders))
	require.Equal(t, initialRouteHeader.Value(), routeHeaders[0].Value())

	t.Log("INVITE okay, sending fake response")

	minimalSDP := []byte("v=0\r\no=- 0 0 IN IP4 127.0.0.1\r\ns=-\r\nc=IN IP4 127.0.0.1\r\nt=0 0\r\nm=audio 5004 RTP/AVP 0\r\na=rtpmap:0 PCMU/8000\r\n")
	response := sip.NewSDPResponseFromRequest(tr.req, minimalSDP)
	require.NotNil(t, response, "NewSDPResponseFromRequest returned nil")
	response.RemoveHeader("Record-Route")
	rr1 := sip.RecordRouteHeader{Address: addedRouteURI}
	rr2 := sip.RecordRouteHeader{Address: initialRouteURI}
	response.AppendHeader(&rr1)
	response.AppendHeader(&rr2)
	sendTestResponse(t, tr, response)

	t.Log("Wait for ACK to be sent")

	// Make sure ACK is okay
	var ackReq *sipRequest
	select {
	case ackReq = <-sipClient.requests:
		// All good
	case <-time.After(100 * time.Millisecond):
		cancel()
		require.Fail(t, "expected ACK request to be created")
		return
	}

	t.Log("Received ACK, validating")

	require.NotNil(t, ackReq)
	require.NotNil(t, ackReq.req)
	require.Equal(t, sip.ACK, ackReq.req.Method)
	require.Equal(t, tr.req.CSeq().SeqNo, ackReq.req.CSeq().SeqNo)
	require.Equal(t, tr.req.CallID(), ackReq.req.CallID())
	ackRouteHeaders := ackReq.req.GetHeaders("Route")
	require.Equal(t, 2, len(ackRouteHeaders)) // We expect this to fail prior to fixing our bug!
	require.Equal(t, initialRouteHeader.Value(), ackRouteHeaders[0].Value())
	require.Equal(t, addedRouteHeader.Value(), ackRouteHeaders[1].Value())

	cancel()
}
