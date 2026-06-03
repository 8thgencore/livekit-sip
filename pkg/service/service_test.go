package service

import (
	"context"
	"testing"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/stretchr/testify/require"
)

type fakeSIPTrunkClient struct {
	deleted []string
}

func (f *fakeSIPTrunkClient) DeleteSIPTrunk(_ context.Context, in *livekit.DeleteSIPTrunkRequest) (*livekit.SIPTrunkInfo, error) {
	f.deleted = append(f.deleted, in.GetSipTrunkId())
	return &livekit.SIPTrunkInfo{SipTrunkId: in.GetSipTrunkId()}, nil
}

func (f *fakeSIPTrunkClient) GetSIPInboundTrunksByIDs(_ context.Context, _ []string) ([]*livekit.SIPInboundTrunkInfo, error) {
	return nil, nil
}

func TestOnSessionEndDeletesOutboundTrunkForAuthFailures(t *testing.T) {
	tests := []struct {
		name     string
		reason   string
		callInfo *livekit.SIPCallInfo
	}{
		{
			name: "register auth loop",
			callInfo: outboundCallInfoWithError(
				"max auth retry attempts reached for SIP register",
			),
		},
		{
			name: "invite auth loop",
			callInfo: outboundCallInfoWithError(
				"max auth retry attempts reached for SIP invite",
			),
		},
		{
			name:   "provider auth reason",
			reason: "provider-auth",
			callInfo: outboundCallInfoWithError(
				"SIP IP trunk rejected INVITE auth with status 407 for address \"sip.novofon.ru\" and from user \"0101536\"",
			),
		},
		{
			name: "provider auth error",
			callInfo: outboundCallInfoWithError(
				"SIP IP trunk rejected INVITE auth with status 407 for address \"sip.novofon.ru\" and from user \"0101536\"",
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deleter := &fakeSIPTrunkClient{}
			svc := &Service{log: logger.GetLogger(), sipClient: deleter}

			svc.OnSessionEnd(context.Background(), nil, tt.callInfo, tt.reason)

			require.Equal(t, []string{"ST_test"}, deleter.deleted)
		})
	}
}

func TestOnSessionEndDeletesOutboundTrunkForForbiddenFailures(t *testing.T) {
	tests := []struct {
		name     string
		reason   string
		callInfo *livekit.SIPCallInfo
	}{
		{
			name:     "forbidden reason",
			reason:   "forbidden",
			callInfo: outboundCallInfoWithStatus(livekit.SIPStatusCode_SIP_STATUS_FORBIDDEN),
		},
		{
			name:     "forbidden status",
			callInfo: outboundCallInfoWithStatus(livekit.SIPStatusCode_SIP_STATUS_FORBIDDEN),
		},
		{
			name:     "forbidden error text",
			callInfo: outboundCallInfoWithError("unexpected status from INVITE response: sip status: 403: Forbidden"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deleter := &fakeSIPTrunkClient{}
			svc := &Service{log: logger.GetLogger(), sipClient: deleter}

			svc.OnSessionEnd(context.Background(), nil, tt.callInfo, tt.reason)

			require.Equal(t, []string{"ST_test"}, deleter.deleted)
		})
	}
}

func TestOnSessionEndDeletesOutboundTrunkForSIPTrunkFailure(t *testing.T) {
	deleter := &fakeSIPTrunkClient{}
	svc := &Service{log: logger.GetLogger(), sipClient: deleter}
	callInfo := outboundCallInfoWithError("sip request timed out")
	callInfo.DisconnectReason = livekit.DisconnectReason_SIP_TRUNK_FAILURE

	svc.OnSessionEnd(context.Background(), nil, callInfo, "request-timeout")

	require.Equal(t, []string{"ST_test"}, deleter.deleted)
}

func TestOnSessionEndDoesNotDeleteOutboundTrunkForNormalFailures(t *testing.T) {
	tests := []struct {
		name     string
		reason   string
		callInfo *livekit.SIPCallInfo
	}{
		{name: "request timeout", callInfo: outboundCallInfoWithStatus(livekit.SIPStatusCode_SIP_STATUS_REQUEST_TIMEOUT)},
		{name: "busy", callInfo: outboundCallInfoWithStatus(livekit.SIPStatusCode_SIP_STATUS_BUSY_HERE)},
		{name: "request terminated", callInfo: outboundCallInfoWithStatus(livekit.SIPStatusCode_SIP_STATUS_REQUEST_TERMINATED)},
		{name: "temporarily unavailable", callInfo: outboundCallInfoWithStatus(livekit.SIPStatusCode_SIP_STATUS_TEMPORARILY_UNAVAILABLE)},
		{name: "declined", callInfo: outboundCallInfoWithStatus(livekit.SIPStatusCode_SIP_STATUS_GLOBAL_DECLINE)},
		{name: "declined invite error text", callInfo: outboundCallInfoWithError("unexpected status from INVITE response: sip status: 603: Declined")},
		{name: "permission denied wrapper with declined sip status", callInfo: outboundCallInfoWithError("SIP dial failed: TwirpError(code=permission_denied, message=twirp error unknown: unexpected status from INVITE response: sip status: 603: Declined, status=403, metadata={'sip_status_code': '603'})")},
		{name: "inbound auth loop", callInfo: inboundCallInfoWithError("max auth retry attempts reached for SIP invite")},
		{name: "empty trunk", callInfo: &livekit.SIPCallInfo{
			CallDirection: livekit.SIPCallDirection_SCD_OUTBOUND,
			Error:         "max auth retry attempts reached for SIP invite",
		}},
		{name: "room disconnected", reason: "room-disconnected", callInfo: outboundCallInfoWithError("")},
		{name: "bye", reason: "bye", callInfo: outboundCallInfoWithError("")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deleter := &fakeSIPTrunkClient{}
			svc := &Service{log: logger.GetLogger(), sipClient: deleter}

			svc.OnSessionEnd(context.Background(), nil, tt.callInfo, tt.reason)

			require.Empty(t, deleter.deleted)
		})
	}
}

func TestOnSessionEndDoesNotDeleteWithoutSIPClient(t *testing.T) {
	svc := &Service{log: logger.GetLogger()}

	svc.OnSessionEnd(context.Background(), nil, outboundCallInfoWithError("max auth retry attempts reached for SIP invite"), "invite-failed")
}

func TestOnSessionEndDeletesOutboundTrunkAfterCall(t *testing.T) {
	deleter := &fakeSIPTrunkClient{}
	svc := &Service{log: logger.GetLogger(), sipClient: deleter}

	svc.OnSessionEnd(context.Background(), nil, outboundCallInfoForHost("voip.sipuni.ru"), "bye")

	require.Equal(t, []string{"ST_test"}, deleter.deleted)
}

func TestOnSessionEndDeletesNonSipuniOutboundTrunkAfterCall(t *testing.T) {
	deleter := &fakeSIPTrunkClient{}
	svc := &Service{log: logger.GetLogger(), sipClient: deleter}

	svc.OnSessionEnd(context.Background(), nil, outboundCallInfoForHost("login.mtt.ru"), "bye")

	require.Equal(t, []string{"ST_test"}, deleter.deleted)
}

func TestOnSessionEndDeletesUiscomOutboundTrunkAfterCall(t *testing.T) {
	deleter := &fakeSIPTrunkClient{}
	svc := &Service{log: logger.GetLogger(), sipClient: deleter}

	svc.OnSessionEnd(context.Background(), nil, outboundCallInfoForHost("pbx.uiscom.ru"), "bye")

	require.Equal(t, []string{"ST_test"}, deleter.deleted)
}

func outboundCallInfoWithError(errText string) *livekit.SIPCallInfo {
	return &livekit.SIPCallInfo{
		CallId:        "SCL_test",
		TrunkId:       "ST_test",
		CallDirection: livekit.SIPCallDirection_SCD_OUTBOUND,
		Error:         errText,
	}
}

func inboundCallInfoWithError(errText string) *livekit.SIPCallInfo {
	return &livekit.SIPCallInfo{
		CallId:        "SCL_test",
		TrunkId:       "ST_test",
		CallDirection: livekit.SIPCallDirection_SCD_INBOUND,
		Error:         errText,
	}
}

func outboundCallInfoWithStatus(status livekit.SIPStatusCode) *livekit.SIPCallInfo {
	info := outboundCallInfoWithError("")
	info.CallStatusCode = &livekit.SIPStatus{Code: status}
	return info
}

func outboundCallInfoForHost(host string) *livekit.SIPCallInfo {
	info := outboundCallInfoWithError("")
	info.ToUri = &livekit.SIPUri{Host: host}
	return info
}
