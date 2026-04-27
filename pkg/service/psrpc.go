package service

import (
	"context"
	"fmt"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/livekit/protocol/rpc"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/livekit/sip/pkg/sip"
)

func optionalStringField(msg protoreflect.Message, names ...protoreflect.Name) string {
	if !msg.IsValid() {
		return ""
	}
	fields := msg.Descriptor().Fields()
	for _, name := range names {
		fd := fields.ByName(name)
		if fd == nil {
			continue
		}
		switch fd.Kind() {
		case protoreflect.StringKind:
			return msg.Get(fd).String()
		}
	}
	return ""
}

func optionalSIPTransportField(msg protoreflect.Message, names ...protoreflect.Name) livekit.SIPTransport {
	if !msg.IsValid() {
		return livekit.SIPTransport_SIP_TRANSPORT_AUTO
	}
	fields := msg.Descriptor().Fields()
	for _, name := range names {
		fd := fields.ByName(name)
		if fd == nil {
			continue
		}
		switch fd.Kind() {
		case protoreflect.EnumKind:
			return livekit.SIPTransport(msg.Get(fd).Enum())
		case protoreflect.Int32Kind, protoreflect.Int64Kind:
			return livekit.SIPTransport(msg.Get(fd).Int())
		case protoreflect.Uint32Kind, protoreflect.Uint64Kind:
			return livekit.SIPTransport(msg.Get(fd).Uint())
		}
	}
	return livekit.SIPTransport_SIP_TRANSPORT_AUTO
}

func GetAuthCredentials(ctx context.Context, psrpcClient rpc.IOInfoClient, call *rpc.SIPCall) (sip.AuthInfo, error) {
	ctx, span := sip.Tracer.Start(ctx, "service.GetAuthCredentials")
	defer span.End()
	resp, err := psrpcClient.GetSIPTrunkAuthentication(ctx, &rpc.GetSIPTrunkAuthenticationRequest{
		Call: call,

		SipCallId:  call.LkCallId,
		From:       call.From.User,
		FromHost:   call.From.Host,
		To:         call.To.User,
		ToHost:     call.To.Host,
		SrcAddress: call.SourceIp,
	})

	if err != nil {
		return sip.AuthInfo{}, err
	}
	registerAddr := optionalStringField(resp.ProtoReflect(),
		"register_address",
		"register_addr",
		"address",
	)
	registerTr := optionalSIPTransportField(resp.ProtoReflect(),
		"register_transport",
		"transport",
	)

	// Handle specific authentication error codes
	switch resp.ErrorCode {
	case rpc.SIPTrunkAuthenticationError_SIP_TRUNK_AUTH_ERROR_QUOTA_EXCEEDED:
		return sip.AuthInfo{
			ProjectID:    resp.ProjectId,
			Result:       sip.AuthQuotaExceeded,
			RegisterAddr: registerAddr,
			RegisterTr:   registerTr,
			ProviderInfo: resp.ProviderInfo,
		}, nil
	case rpc.SIPTrunkAuthenticationError_SIP_TRUNK_AUTH_ERROR_NO_TRUNK_FOUND:
		return sip.AuthInfo{
			ProjectID:    resp.ProjectId,
			Result:       sip.AuthNoTrunkFound,
			RegisterAddr: registerAddr,
			RegisterTr:   registerTr,
			ProviderInfo: resp.ProviderInfo,
		}, nil
	}

	if resp.Drop {
		return sip.AuthInfo{
			ProjectID:    resp.ProjectId,
			Result:       sip.AuthDrop,
			RegisterAddr: registerAddr,
			RegisterTr:   registerTr,
			ProviderInfo: resp.ProviderInfo,
		}, nil
	}
	if resp.Username != "" && resp.Password != "" {
		return sip.AuthInfo{
			ProjectID:    resp.ProjectId,
			TrunkID:      resp.SipTrunkId,
			Result:       sip.AuthPassword,
			Username:     resp.Username,
			Password:     resp.Password,
			RegisterAddr: registerAddr,
			RegisterTr:   registerTr,
			ProviderInfo: resp.ProviderInfo,
		}, nil
	}
	return sip.AuthInfo{
		ProjectID:    resp.ProjectId,
		TrunkID:      resp.SipTrunkId,
		Result:       sip.AuthAccept,
		RegisterAddr: registerAddr,
		RegisterTr:   registerTr,
		ProviderInfo: resp.ProviderInfo,
	}, nil
}

func DispatchCall(ctx context.Context, psrpcClient rpc.IOInfoClient, log logger.Logger, info *sip.CallInfo) sip.CallDispatch {
	ctx, span := sip.Tracer.Start(ctx, "service.DispatchCall")
	defer span.End()
	resp, err := psrpcClient.EvaluateSIPDispatchRules(ctx, &rpc.EvaluateSIPDispatchRulesRequest{
		SipTrunkId: info.TrunkID,
		Call:       info.Call,
		Pin:        info.Pin,
		NoPin:      info.NoPin,

		SipCallId:     info.Call.LkCallId,
		CallingNumber: info.Call.From.User,
		CallingHost:   info.Call.From.Host,
		CalledNumber:  info.Call.To.User,
		CalledHost:    info.Call.To.Host,
		SrcAddress:    info.Call.SourceIp,
	})

	if err != nil {
		log.Warnw("SIP handle dispatch rule error", err)
		return sip.CallDispatch{Result: sip.DispatchServiceUnavailable}
	}
	switch resp.Result {
	default:
		log.Errorw("SIP handle dispatch rule error", fmt.Errorf("unexpected dispatch result: %v", resp.Result))
		return sip.CallDispatch{
			ProjectID: resp.ProjectId,
			TrunkID:   resp.SipTrunkId,
			Result:    sip.DispatchNoRuleReject,
		}
	case rpc.SIPDispatchResult_LEGACY_ACCEPT_OR_PIN:
		if resp.RequestPin {
			return sip.CallDispatch{
				ProjectID:       resp.ProjectId,
				TrunkID:         resp.SipTrunkId,
				DispatchRuleID:  resp.SipDispatchRuleId,
				Result:          sip.DispatchRequestPin,
				MediaEncryption: resp.MediaEncryption,
			}
		}
		// TODO: finally deprecate and drop
		return sip.CallDispatch{
			ProjectID: resp.ProjectId,
			Result:    sip.DispatchAccept,
			Room: sip.RoomConfig{
				WsUrl:    resp.WsUrl,
				Token:    resp.Token,
				RoomName: resp.RoomName,
				Participant: sip.ParticipantConfig{
					Identity:   resp.ParticipantIdentity,
					Name:       resp.ParticipantName,
					Metadata:   resp.ParticipantMetadata,
					Attributes: resp.ParticipantAttributes,
				},
				RoomPreset: resp.RoomPreset,
				RoomConfig: resp.RoomConfig,
			},
			TrunkID:             resp.SipTrunkId,
			DispatchRuleID:      resp.SipDispatchRuleId,
			Headers:             resp.Headers,
			IncludeHeaders:      resp.IncludeHeaders,
			HeadersToAttributes: resp.HeadersToAttributes,
			AttributesToHeaders: resp.AttributesToHeaders,
			EnabledFeatures:     resp.EnabledFeatures,
			RingingTimeout:      resp.RingingTimeout.AsDuration(),
			MaxCallDuration:     resp.MaxCallDuration.AsDuration(),
			MediaEncryption:     resp.MediaEncryption,
		}
	case rpc.SIPDispatchResult_ACCEPT:
		return sip.CallDispatch{
			ProjectID: resp.ProjectId,
			Result:    sip.DispatchAccept,
			Room: sip.RoomConfig{
				WsUrl:    resp.WsUrl,
				Token:    resp.Token,
				RoomName: resp.RoomName,
				Participant: sip.ParticipantConfig{
					Identity:   resp.ParticipantIdentity,
					Name:       resp.ParticipantName,
					Metadata:   resp.ParticipantMetadata,
					Attributes: resp.ParticipantAttributes,
				},
				RoomPreset: resp.RoomPreset,
				RoomConfig: resp.RoomConfig,
			},
			TrunkID:             resp.SipTrunkId,
			DispatchRuleID:      resp.SipDispatchRuleId,
			Headers:             resp.Headers,
			IncludeHeaders:      resp.IncludeHeaders,
			HeadersToAttributes: resp.HeadersToAttributes,
			AttributesToHeaders: resp.AttributesToHeaders,
			EnabledFeatures:     resp.EnabledFeatures,
			FeatureFlags:        resp.FeatureFlags,
			RingingTimeout:      resp.RingingTimeout.AsDuration(),
			MaxCallDuration:     resp.MaxCallDuration.AsDuration(),
			MediaEncryption:     resp.MediaEncryption,
		}
	case rpc.SIPDispatchResult_REQUEST_PIN:
		return sip.CallDispatch{
			ProjectID:       resp.ProjectId,
			Result:          sip.DispatchRequestPin,
			TrunkID:         resp.SipTrunkId,
			MediaEncryption: resp.MediaEncryption,
		}
	case rpc.SIPDispatchResult_REJECT:
		return sip.CallDispatch{
			ProjectID: resp.ProjectId,
			Result:    sip.DispatchNoRuleReject,
			TrunkID:   resp.SipTrunkId,
		}
	case rpc.SIPDispatchResult_DROP:
		return sip.CallDispatch{
			ProjectID: resp.ProjectId,
			Result:    sip.DispatchNoRuleDrop,
			TrunkID:   resp.SipTrunkId,
		}
	}
}
