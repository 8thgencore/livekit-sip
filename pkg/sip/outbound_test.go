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
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
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

func sendProxyAuthRequired(t *testing.T, tx *transactionRequest) {
	t.Helper()
	challenge := digest.Challenge{
		Realm: "sip.nvfn.ru",
		Nonce: "12345678901234567890123456789012",
	}
	resp := sip.NewResponseFromRequest(tx.req, sip.StatusProxyAuthRequired, "Proxy Authentication Required", nil)
	resp.AppendHeader(sip.NewHeader("Proxy-Authenticate", challenge.String()))
	require.NoError(t, tx.transaction.SendResponse(resp))
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
	require.NoError(t, registerTx.transaction.SendResponse(sip.NewResponseFromRequest(registerTx.req, sip.StatusOK, "OK", nil)))

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
	require.NoError(t, inviteTx.transaction.SendResponse(sip.NewResponseFromRequest(inviteTx.req, sip.StatusForbidden, "Forbidden", nil)))

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

	done := make(chan error, 1)
	go func() {
		_, err := client.CreateSIPParticipant(context.Background(), req)
		done <- err
	}()

	registerTx := waitTransaction(t, sipClient)
	require.Equal(t, sip.REGISTER, registerTx.req.Method)
	require.NoError(t, registerTx.transaction.SendResponse(sip.NewResponseFromRequest(registerTx.req, sip.StatusOK, "OK", nil)))

	registeredInviteTx := waitTransaction(t, sipClient)
	require.Equal(t, sip.INVITE, registeredInviteTx.req.Method)
	require.Equal(t, mockSIPHost, registeredInviteTx.req.From().Address.Host)
	require.Equal(t, mockRegisteredUser, registeredInviteTx.req.Contact().Address.User)
	require.NoError(t, registeredInviteTx.transaction.SendResponse(sip.NewResponseFromRequest(registeredInviteTx.req, sip.StatusBusyHere, "Busy Here", nil)))

	directInviteTx := waitTransaction(t, sipClient)
	require.Equal(t, sip.INVITE, directInviteTx.req.Method)
	require.Equal(t, client.sconf.SignalingIP.String(), directInviteTx.req.From().Address.Host)
	require.NotZero(t, directInviteTx.req.From().Address.Port)
	require.Empty(t, directInviteTx.req.Contact().Address.User)
	require.NoError(t, directInviteTx.transaction.SendResponse(sip.NewResponseFromRequest(directInviteTx.req, sip.StatusBusyHere, "Busy Here", nil)))

	require.Error(t, <-done)
}

func TestOutboundInviteAutoKeepsRegistrationForUISBusyHere(t *testing.T) {
	const (
		mockRegisteredUser = "0526470"
		mockCallToUser     = "+77057756019"
		mockSIPHost        = "pbx.uiscom.ru"
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

	done := make(chan error, 1)
	go func() {
		_, err := client.CreateSIPParticipant(context.Background(), req)
		done <- err
	}()

	registerTx := waitTransaction(t, sipClient)
	require.Equal(t, sip.REGISTER, registerTx.req.Method)
	require.NoError(t, registerTx.transaction.SendResponse(sip.NewResponseFromRequest(registerTx.req, sip.StatusOK, "OK", nil)))

	registeredInviteTx := waitTransaction(t, sipClient)
	require.Equal(t, sip.INVITE, registeredInviteTx.req.Method)
	require.Equal(t, mockSIPHost, registeredInviteTx.req.From().Address.Host)
	require.Equal(t, mockRegisteredUser, registeredInviteTx.req.Contact().Address.User)
	require.NoError(t, registeredInviteTx.transaction.SendResponse(sip.NewResponseFromRequest(registeredInviteTx.req, sip.StatusBusyHere, "Busy Here", nil)))

	select {
	case tx := <-sipClient.transactions:
		t.Fatalf("unexpected immediate retry without REGISTER profile: %s contact=%s", tx.req.Method, tx.req.Contact().Address.String())
	case <-time.After(500 * time.Millisecond):
	}

	retryInviteTx := waitTransactionWithTimeout(t, sipClient, inviteRetryAfterBusyDelay+time.Second)
	require.Equal(t, sip.INVITE, retryInviteTx.req.Method)
	require.Equal(t, mockSIPHost, retryInviteTx.req.From().Address.Host)
	require.Equal(t, mockRegisteredUser, retryInviteTx.req.Contact().Address.User)
	require.NoError(t, retryInviteTx.transaction.SendResponse(sip.NewResponseFromRequest(retryInviteTx.req, sip.StatusBusyHere, "Busy Here", nil)))

	require.Error(t, <-done)
}

func TestRetryInviteAfterFreshRegisterForceReregistersOnTemporaryFailures(t *testing.T) {
	tests := []struct {
		name   string
		status sip.StatusCode
		reason string
	}{
		{name: "temporarily unavailable", status: sip.StatusTemporarilyUnavailable, reason: "Temporarily Unavailable"},
		{name: "service unavailable", status: sip.StatusServiceUnavailable, reason: "Service Unavailable"},
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
			require.NoError(t, registerTx.transaction.SendResponse(sip.NewResponseFromRequest(registerTx.req, sip.StatusOK, "OK", nil)))

			res := <-done
			require.NoError(t, res.err)
			require.True(t, res.retried)
		})
	}
}

func TestRetryInviteAfterBusyUsesFreshCallIDAndHandlesAuth(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)
	outbound := newTestOutboundCall(client)
	toURI := CreateURIFromUserAndAddress("+79993441100", "sip.novofon.ru:5060", TransportUDP)

	firstResult := make(chan error, 1)
	go func() {
		_, err := outbound.cc.Invite(context.Background(), toURI, nil, "0101536", "test-password", nil, []byte("v=0\r\n"), nil)
		firstResult <- err
	}()

	firstInvite := waitTransaction(t, sipClient)
	require.Equal(t, sip.INVITE, firstInvite.req.Method)
	firstCallID := firstInvite.req.CallID().Value()
	require.Equal(t, uint32(1), firstInvite.req.CSeq().SeqNo)
	require.Nil(t, firstInvite.req.GetHeader("Proxy-Authorization"))
	sendProxyAuthRequired(t, firstInvite)

	firstAuthInvite := waitTransaction(t, sipClient)
	require.Equal(t, firstCallID, firstAuthInvite.req.CallID().Value())
	require.Equal(t, uint32(2), firstAuthInvite.req.CSeq().SeqNo)
	require.NotNil(t, firstAuthInvite.req.GetHeader("Proxy-Authorization"))
	require.NoError(t, firstAuthInvite.transaction.SendResponse(sip.NewResponseFromRequest(firstAuthInvite.req, sip.StatusBusyHere, "Busy Here", nil)))

	firstErr := <-firstResult
	require.Error(t, firstErr)
	require.True(t, outbound.shouldRetryInviteAfterBusy(firstErr))

	retryResult := make(chan struct {
		body []byte
		err  error
	}, 1)
	go func() {
		body, err := outbound.retryInviteAfterBusy(context.Background(), toURI, []byte("v=0\r\n"), nil)
		retryResult <- struct {
			body []byte
			err  error
		}{body: body, err: err}
	}()

	retryInvite := waitTransactionWithTimeout(t, sipClient, inviteRetryAfterBusyDelay+time.Second)
	require.Equal(t, sip.INVITE, retryInvite.req.Method)
	retryCallID := retryInvite.req.CallID().Value()
	require.NotEqual(t, firstCallID, retryCallID)
	require.Equal(t, uint32(1), retryInvite.req.CSeq().SeqNo)
	require.Nil(t, retryInvite.req.GetHeader("Proxy-Authorization"))
	sendProxyAuthRequired(t, retryInvite)

	retryAuthInvite := waitTransaction(t, sipClient)
	require.Equal(t, retryCallID, retryAuthInvite.req.CallID().Value())
	require.Equal(t, uint32(2), retryAuthInvite.req.CSeq().SeqNo)
	require.NotNil(t, retryAuthInvite.req.GetHeader("Proxy-Authorization"))
	answer := []byte("v=0\r\n")
	require.NoError(t, retryAuthInvite.transaction.SendResponse(sip.NewResponseFromRequest(retryAuthInvite.req, sip.StatusOK, "OK", answer)))

	res := <-retryResult
	require.NoError(t, res.err)
	require.Equal(t, answer, res.body)
}

func TestOutboundInviteDigestURIUsesRequestURIWithPort(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)
	outbound := newTestOutboundCall(client)
	toURI := CreateURIFromUserAndAddress("+79998887722", "login.test.com:5060", TransportUDP)

	result := make(chan error, 1)
	go func() {
		_, err := outbound.cc.Invite(context.Background(), toURI, nil, "123456", "test-password", nil, []byte("v=0\r\n"), nil)
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
	require.NoError(t, firstInvite.transaction.SendResponse(resp))

	authInvite := waitTransaction(t, sipClient)
	require.Equal(t, sip.INVITE, authInvite.req.Method)
	authHeader := authInvite.req.GetHeader("Authorization")
	require.NotNil(t, authHeader)
	cred, err := digest.ParseCredentials(authHeader.Value())
	require.NoError(t, err)
	require.Equal(t, firstInvite.req.Recipient.String(), cred.URI)
	require.Equal(t, "sip:+79998887722@login.test.com:5060;transport=udp", cred.URI)

	require.NoError(t, authInvite.transaction.SendResponse(sip.NewResponseFromRequest(authInvite.req, sip.StatusOK, "OK", []byte("v=0\r\n"))))
	require.NoError(t, <-result)
}

func TestRetryInviteAfterBusyReturnsSecondBusy(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)
	outbound := newTestOutboundCall(client)
	toURI := CreateURIFromUserAndAddress("+79993441100", "sip.novofon.ru:5060", TransportUDP)

	retryResult := make(chan error, 1)
	go func() {
		_, err := outbound.retryInviteAfterBusy(context.Background(), toURI, []byte("v=0\r\n"), nil)
		retryResult <- err
	}()

	retryInvite := waitTransactionWithTimeout(t, sipClient, inviteRetryAfterBusyDelay+time.Second)
	require.Equal(t, sip.INVITE, retryInvite.req.Method)
	require.Equal(t, uint32(1), retryInvite.req.CSeq().SeqNo)
	require.NoError(t, retryInvite.transaction.SendResponse(sip.NewResponseFromRequest(retryInvite.req, sip.StatusBusyHere, "Busy Here", nil)))

	err := <-retryResult
	require.Error(t, err)
	var sipErr *livekit.SIPStatus
	require.ErrorAs(t, err, &sipErr)
	require.Equal(t, livekit.SIPStatusCode_SIP_STATUS_BUSY_HERE, sipErr.Code)
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
	require.NoError(t, inviteTx.transaction.SendResponse(sip.NewResponseFromRequest(inviteTx.req, sip.StatusRequestTerminated, "Request Terminated", nil)))

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
	require.NoError(t, inviteTx.transaction.SendResponse(sip.NewResponseFromRequest(inviteTx.req, sip.StatusRequestTimeout, "Request Timeout", nil)))

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
	require.NoError(t, inviteTx.transaction.SendResponse(sip.NewResponseFromRequest(inviteTx.req, sip.StatusForbidden, "Forbidden", nil)))

	require.Error(t, <-done)
}

func TestOutboundInviteAutoSkipsRegisterForConfiguredHost(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)
	const skipRegisterHost = "sip.no-register.example"
	outboundRegisterSkipHosts[skipRegisterHost] = struct{}{}
	t.Cleanup(func() { delete(outboundRegisterSkipHosts, skipRegisterHost) })

	req := MinimalCreateSIPParticipantRequest()
	req.Address = skipRegisterHost + ":5060"
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
	require.NoError(t, inviteTx.transaction.SendResponse(sip.NewResponseFromRequest(inviteTx.req, sip.StatusForbidden, "Forbidden", nil)))
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
	require.NoError(t, inviteTx.transaction.SendResponse(sip.NewResponseFromRequest(inviteTx.req, sip.StatusForbidden, "Forbidden", nil)))
	require.Error(t, <-done)
}

func TestOutboundInviteRegisterSkippedIPTrunk407ReturnsProviderAuthErrorAfterRetries(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)
	const skipRegisterHost = "sip.no-register.example"
	outboundRegisterSkipHosts[skipRegisterHost] = struct{}{}
	t.Cleanup(func() { delete(outboundRegisterSkipHosts, skipRegisterHost) })

	req := MinimalCreateSIPParticipantRequest()
	req.Address = skipRegisterHost + ":5060"
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
	require.NoError(t, registerTx.transaction.SendResponse(sip.NewResponseFromRequest(registerTx.req, sip.StatusForbidden, "Forbidden", nil)))
	require.Error(t, <-done)
}

func TestOutboundInviteWithoutRegistrationKeepsHostOnlyContact(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)
	req := MinimalCreateSIPParticipantRequest()
	req.WaitUntilAnswered = true

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
	require.NoError(t, inviteTx.transaction.SendResponse(sip.NewResponseFromRequest(inviteTx.req, sip.StatusForbidden, "Forbidden", nil)))

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
	tr.transaction.SendResponse(response)

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
