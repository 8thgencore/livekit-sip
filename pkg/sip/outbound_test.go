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

	"github.com/livekit/sipgo/sip"
)

func setOutboundRegisterMode(req interface{ ProtoReflect() protoreflect.Message }, mode outboundRegisterMode) {
	msg := req.ProtoReflect()
	unknown := msg.GetUnknown()
	unknown = protowire.AppendTag(unknown, internalCreateSIPParticipantRegisterModeField, protowire.VarintType)
	unknown = protowire.AppendVarint(unknown, uint64(mode))
	msg.SetUnknown(unknown)
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
	require.NoError(t, inviteTx.transaction.SendResponse(sip.NewResponseFromRequest(inviteTx.req, sip.StatusBusyHere, "Busy Here", nil)))

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
	require.NoError(t, inviteTx.transaction.SendResponse(sip.NewResponseFromRequest(inviteTx.req, sip.StatusBusyHere, "Busy Here", nil)))

	require.Error(t, <-done)
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
	require.NoError(t, inviteTx.transaction.SendResponse(sip.NewResponseFromRequest(inviteTx.req, sip.StatusBusyHere, "Busy Here", nil)))

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
