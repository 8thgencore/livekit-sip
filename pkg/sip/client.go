// Copyright 2023 LiveKit, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 	http://www.apache.org/licenses/LICENSE-2.0
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
	"log/slog"
	"net"
	"net/netip"
	"strings"
	"sync"
	"time"

	"github.com/frostbyte73/core"
	"golang.org/x/exp/maps"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/livekit/protocol/rpc"
	"github.com/livekit/protocol/utils/traceid"
	"github.com/livekit/psrpc"
	"github.com/livekit/sipgo"
	"github.com/livekit/sipgo/sip"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/livekit/sip/pkg/config"
	siperrors "github.com/livekit/sip/pkg/errors"
	"github.com/livekit/sip/pkg/stats"
)

// SIPClient is an interface mirroring sipgo.Client to be able to mock it in tests.
//
// Note: *sipgo.Client implements this interface directly, so no wrapper is needed.
type SIPClient interface {
	TransactionRequest(req *sip.Request, options ...sipgo.ClientRequestOption) (sip.ClientTransaction, error)
	WriteRequest(req *sip.Request, options ...sipgo.ClientRequestOption) error
	Close() error
}

type GetSipClientFunc func(ua *sipgo.UserAgent, options ...sipgo.ClientOption) (SIPClient, error)

func DefaultGetSipClientFunc(ua *sipgo.UserAgent, options ...sipgo.ClientOption) (SIPClient, error) {
	return sipgo.NewClient(ua, options...)
}

type RegistrarResolver func(context.Context, string) ([]netip.Addr, error)

func defaultRegistrarResolver(ctx context.Context, host string) ([]netip.Addr, error) {
	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	addrs := make([]netip.Addr, 0, len(ips))
	for _, ip := range ips {
		addr, ok := netip.AddrFromSlice(ip.IP)
		if ok {
			addrs = append(addrs, addr.Unmap())
		}
	}
	return addrs, nil
}

type Client struct {
	conf   *config.Config
	sconf  *ServiceConfig
	log    logger.Logger
	region string
	mon    *stats.Monitor

	sipCli SIPClient

	closing     core.Fuse
	cmu         sync.Mutex
	activeCalls map[LocalTag]*outboundCall
	trunkQueues *outboundTrunkQueueManager

	registrationManager *RegistrationManager

	handler          Handler
	getIOClient      GetIOInfoClient
	getSipClient     GetSipClientFunc
	getRoom          GetRoomFunc
	resolveRegistrar RegistrarResolver
}

const internalCreateSIPParticipantRegisterModeField protowire.Number = 34
const internalCreateSIPParticipantRegisterModeName protoreflect.Name = "register_mode"

func outboundRegisterModeFromRequest(req *rpc.InternalCreateSIPParticipantRequest) outboundRegisterMode {
	if req == nil {
		return outboundRegisterModeRequired
	}
	msg := req.ProtoReflect()
	if fd := msg.Descriptor().Fields().ByName(internalCreateSIPParticipantRegisterModeName); fd != nil {
		switch fd.Kind() {
		case protoreflect.EnumKind:
			return normalizeOutboundRegisterMode(uint64(msg.Get(fd).Enum()))
		case protoreflect.Int32Kind, protoreflect.Int64Kind:
			return normalizeOutboundRegisterMode(uint64(msg.Get(fd).Int()))
		case protoreflect.Uint32Kind, protoreflect.Uint64Kind:
			return normalizeOutboundRegisterMode(msg.Get(fd).Uint())
		}
	}
	b := msg.GetUnknown()
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return outboundRegisterModeRequired
		}
		b = b[n:]
		if num != internalCreateSIPParticipantRegisterModeField || typ != protowire.VarintType {
			n = protowire.ConsumeFieldValue(num, typ, b)
			if n < 0 {
				return outboundRegisterModeRequired
			}
			b = b[n:]
			continue
		}
		v, n := protowire.ConsumeVarint(b)
		if n < 0 {
			return outboundRegisterModeRequired
		}
		return normalizeOutboundRegisterMode(v)
	}
	return outboundRegisterModeRequired
}

func normalizeOutboundRegisterMode(v uint64) outboundRegisterMode {
	switch outboundRegisterMode(v) {
	case outboundRegisterModeAuto, outboundRegisterModeDisabled, outboundRegisterModeRequired:
		return outboundRegisterMode(v)
	default:
		return outboundRegisterModeRequired
	}
}

type ClientOption func(c *Client)

func WithGetSipClient(fn GetSipClientFunc) ClientOption {
	return func(c *Client) {
		if fn != nil {
			c.getSipClient = fn
		}
	}
}

func WithGetRoomClient(fn GetRoomFunc) ClientOption {
	return func(c *Client) {
		if fn != nil {
			c.getRoom = fn
		}
	}
}

func WithRegistrarResolver(fn RegistrarResolver) ClientOption {
	return func(c *Client) {
		if fn != nil {
			c.resolveRegistrar = fn
		}
	}
}

func NewClient(region string, conf *config.Config, log logger.Logger, mon *stats.Monitor, getIOClient GetIOInfoClient, options ...ClientOption) *Client {
	if log == nil {
		log = logger.GetLogger()
	}
	c := &Client{
		conf:                conf,
		log:                 log,
		region:              region,
		mon:                 mon,
		getIOClient:         getIOClient,
		registrationManager: NewRegistrationManager(),
		getSipClient:        DefaultGetSipClientFunc,
		getRoom:             DefaultGetRoomFunc,
		resolveRegistrar:    defaultRegistrarResolver,
		activeCalls:         make(map[LocalTag]*outboundCall),
		trunkQueues:         newOutboundTrunkQueueManager(mon),
	}
	for _, option := range options {
		option(c)
	}
	return c
}

func (c *Client) Start(agent *sipgo.UserAgent, sc *ServiceConfig) error {
	c.sconf = sc
	c.log.Infow("client starting", "local", c.sconf.SignalingIPLocal, "external", c.sconf.SignalingIP)

	if agent == nil {
		ua, err := sipgo.NewUA(
			sipgo.WithUserAgent(UserAgent),
			sipgo.WithUserAgentLogger(slog.New(logger.ToSlogHandler(c.log))),
		)
		if err != nil {
			return err
		}
		agent = ua
	}

	var err error
	c.sipCli, err = c.getSipClient(agent,
		sipgo.WithClientHostname(c.sconf.SignalingIP.String()),
		sipgo.WithClientLogger(slog.New(logger.ToSlogHandler(c.log))),
	)
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) Stop() {
	ctx := context.Background()
	ctx, span := Tracer.Start(ctx, "sip.Client.Stop")
	defer span.End()
	c.closing.Break()
	c.cmu.Lock()
	calls := maps.Values(c.activeCalls)
	c.activeCalls = make(map[LocalTag]*outboundCall)
	c.cmu.Unlock()
	for _, call := range calls {
		call.Close(ctx)
	}
	if c.trunkQueues != nil {
		c.trunkQueues.Stop()
	}
	if c.sipCli != nil {
		c.sipCli.Close()
		c.sipCli = nil
	}
}

func (c *Client) SetHandler(handler Handler) {
	c.handler = handler
}

func (c *Client) ContactURI(tr Transport) URI {
	return getContactURI(c.conf, c.sconf.SignalingIP, tr)
}

func (c *Client) CreateSIPParticipant(ctx context.Context, req *rpc.InternalCreateSIPParticipantRequest) (*rpc.InternalCreateSIPParticipantResponse, error) {
	ctx, span := Tracer.Start(ctx, "Client.CreateSIPParticipant")
	defer span.End()
	return c.createSIPParticipant(ctx, req)
}

func (c *Client) getActiveCall(tag LocalTag) *outboundCall {
	c.cmu.Lock()
	defer c.cmu.Unlock()
	return c.activeCalls[tag]
}

func (c *Client) createSIPParticipant(ctx context.Context, req *rpc.InternalCreateSIPParticipantRequest) (resp *rpc.InternalCreateSIPParticipantResponse, retErr error) {
	if c.mon.Health() != stats.HealthOK {
		return nil, siperrors.ErrUnavailable
	}
	req.Upgrade()
	req.Address = normalizeRepeatedSIPPort(req.Address)
	req.Hostname = normalizeSIPHostname(req.Hostname)
	if req.CallTo == "" {
		return nil, psrpc.NewErrorf(psrpc.InvalidArgument, "call-to number must be set")
	} else if req.Address == "" {
		return nil, psrpc.NewErrorf(psrpc.InvalidArgument, "trunk adresss must be set")
	} else if req.Number == "" {
		return nil, psrpc.NewErrorf(psrpc.InvalidArgument, "trunk outbound number must be set")
	} else if req.RoomName == "" {
		return nil, psrpc.NewErrorf(psrpc.InvalidArgument, "room name must be set")
	}
	if strings.Contains(req.CallTo, "@") {
		return nil, psrpc.NewErrorf(psrpc.InvalidArgument, "call_to should be a phone number or SIP user, not a full SIP URI")
	}
	if strings.HasPrefix(req.Address, "sip:") || strings.HasPrefix(req.Address, "sips:") {
		return nil, psrpc.NewErrorf(psrpc.InvalidArgument, "address must be a hostname without 'sip:' prefix")
	}
	if strings.Contains(req.Address, "transport=") {
		return nil, psrpc.NewErrorf(psrpc.InvalidArgument, "address must not contain parameters; use transport field")
	}
	if strings.ContainsAny(req.Address, ";=") {
		return nil, psrpc.NewErrorf(psrpc.InvalidArgument, "address must not contain parameters")
	}
	log := c.log
	if req.ProjectId != "" {
		log = log.WithValues("projectID", req.ProjectId)
	}
	if req.SipTrunkId != "" {
		log = log.WithValues("sipTrunk", req.SipTrunkId)
	}
	mconf, err := newMediaConfig(req.Media, c.conf.MediaTimeout)
	if err != nil {
		return nil, err
	}
	mconf = applyOutboundProviderMediaProfile(req.Address, req.Media, mconf)
	tid := traceid.FromGUID(req.SipCallId)
	log = log.WithValues(
		"callID", req.SipCallId,
		"traceID", tid.String(),
		"room", req.RoomName,
		"participant", req.ParticipantIdentity,
		"participantName", req.ParticipantName,
		"fromHost", req.Hostname,
		"fromUser", req.Number,
		"toHost", req.Address,
		"toUser", req.CallTo,
		"direction", "outbound",
		"registerMode", outboundRegisterModeFromRequest(req).String(),
	)

	req.ParticipantAttributes = maps.Clone(req.ParticipantAttributes) // shallow clone - string/string map. Needed to avoid mutating psrpc req
	state := NewCallState(c.getIOClient(req.ProjectId), c.createSIPCallInfo(req))

	defer func() {
		state.Update(ctx, func(info *livekit.SIPCallInfo) {

			switch retErr {
			case nil:
				info.CallStatus = livekit.SIPCallStatus_SCS_PARTICIPANT_JOINED
			default:
				info.CallStatus = livekit.SIPCallStatus_SCS_ERROR
				info.DisconnectReason = livekit.DisconnectReason_UNKNOWN_REASON
				info.Error = retErr.Error()
			}
		})
	}()

	roomConf := RoomConfig{
		WsUrl:    req.WsUrl,
		Token:    req.Token,
		RoomName: req.RoomName,
		Participant: ParticipantConfig{
			Identity:   req.ParticipantIdentity,
			Name:       req.ParticipantName,
			Metadata:   req.ParticipantMetadata,
			Attributes: req.ParticipantAttributes,
		},
	}
	sipConf := sipOutboundConfig{
		address:         req.Address,
		transport:       req.Transport,
		host:            req.Hostname,
		from:            req.Number,
		to:              req.CallTo,
		user:            req.Username,
		pass:            req.Password,
		dtmf:            req.Dtmf,
		dialtone:        req.PlayDialtone,
		headers:         maps.Clone(req.Headers), // shallow clone - string/string map. Needed to avoid mutating psrpc req
		includeHeaders:  req.IncludeHeaders,
		headersToAttrs:  req.HeadersToAttributes,
		attrsToHeaders:  req.AttributesToHeaders,
		ringingTimeout:  req.RingingTimeout.AsDuration(),
		maxCallDuration: req.MaxCallDuration.AsDuration(),
		enabledFeatures: req.EnabledFeatures,
		featureFlags:    req.FeatureFlags,
		mediaConfig:     mconf,
		displayName:     req.DisplayName,
		registerMode:    outboundRegisterModeFromRequest(req),
	}
	if req.FeatureFlags[outboundRouteHeadersFeatureFlag] == "true" {
		sipConf.routeHeaders = cloneRouteHeaders(c.conf.OutboundRouteHeaders)
	}
	log.Infow("Creating SIP participant")
	trunkKey := outboundTrunkQueueKey(req)
	trunkMaxConcurrentCalls := outboundTrunkQueueMaxConcurrentCalls(req)
	queueStatus := c.trunkQueues.Status(trunkKey)
	log.Infow("waiting for outbound trunk slot",
		"trunkKey", trunkKey,
		"maxConcurrent", trunkMaxConcurrentCalls,
		"queued", queueStatus.Waiting,
		"active", queueStatus.Running,
		"acquired", false,
	)
	queueStart := time.Now()
	releaseTrunkSlot, err := c.trunkQueues.Acquire(ctx, trunkKey, trunkMaxConcurrentCalls)
	if err != nil {
		queueStatus = c.trunkQueues.Status(trunkKey)
		log.Warnw("failed to acquire outbound trunk slot", err,
			"trunkKey", trunkKey,
			"maxConcurrent", trunkMaxConcurrentCalls,
			"queueWaitMs", time.Since(queueStart).Milliseconds(),
			"queued", queueStatus.Waiting,
			"active", queueStatus.Running,
			"acquired", false,
		)
		return nil, err
	}
	queueStatus = c.trunkQueues.Status(trunkKey)
	log.Infow("acquired outbound trunk slot",
		"trunkKey", trunkKey,
		"maxConcurrent", trunkMaxConcurrentCalls,
		"queueWaitMs", time.Since(queueStart).Milliseconds(),
		"queued", queueStatus.Waiting,
		"active", queueStatus.Running,
		"acquired", true,
	)

	call, err := c.newCall(ctx, tid, c.conf, log, LocalTag(req.SipCallId), roomConf, sipConf, state, req.ProjectId)
	if err != nil {
		releaseTrunkSlot()
		return nil, err
	}
	call.releaseTrunkSlot = releaseTrunkSlot
	p := call.Participant()
	// Start actual SIP call async.

	info := &rpc.InternalCreateSIPParticipantResponse{
		ParticipantId:       p.ID,
		ParticipantIdentity: p.Identity,
		SipCallId:           req.SipCallId,
	}
	if !req.WaitUntilAnswered {
		call.DialAsync(ctx)
		return info, nil
	}
	if err := call.Dial(ctx); err != nil {
		return nil, err
	}
	go call.WaitClose(context.WithoutCancel(ctx))
	return info, nil
}

func (c *Client) createSIPCallInfo(req *rpc.InternalCreateSIPParticipantRequest) *livekit.SIPCallInfo {
	toUri := CreateURIFromUserAndAddress(req.CallTo, req.Address, TransportFrom(req.Transport))
	fromiUri := URI{
		User: req.Number,
		Host: req.Hostname,
		Addr: netip.AddrPortFrom(c.sconf.SignalingIP, uint16(c.conf.SIPPort)),
	}

	callInfo := &livekit.SIPCallInfo{
		CallId:                req.SipCallId,
		Region:                c.region,
		TrunkId:               req.SipTrunkId,
		RoomName:              req.RoomName,
		ParticipantIdentity:   req.ParticipantIdentity,
		ParticipantAttributes: req.ParticipantAttributes,
		CallDirection:         livekit.SIPCallDirection_SCD_OUTBOUND,
		ToUri:                 toUri.ToSIPUri(),
		FromUri:               fromiUri.ToSIPUri(),
		CreatedAtNs:           time.Now().UnixNano(),
		MediaEncryption:       req.MediaEncryption.String(),
		EnabledFeatures:       req.EnabledFeatures,
	}

	return callInfo
}

func (c *Client) OnRequest(req *sip.Request, tx sip.ServerTransaction) bool {
	c.log.Debugw("received SIP request",
		"method", req.Method,
		"callID", requestCallID(req),
		"fromTag", requestFromTag(req),
		"toTag", requestToTag(req),
	)
	switch req.Method {
	default:
		return false
	case "BYE":
		return c.onBye(req, tx)
	case "NOTIFY":
		return c.onNotify(req, tx)
	}
}

func (c *Client) getActiveCallForRequest(req *sip.Request) (*outboundCall, string) {
	if tag, err := GetLocalTagUAS(req); err == nil && tag != "" {
		c.cmu.Lock()
		call := c.activeCalls[tag]
		c.cmu.Unlock()
		if call != nil {
			return call, "to-tag"
		}
	}

	if tag := requestFromTag(req); tag != "" {
		c.cmu.Lock()
		call := c.activeCalls[LocalTag(tag)]
		c.cmu.Unlock()
		if call != nil {
			return call, "from-tag"
		}
	}

	callID := requestCallID(req)
	if callID == "" {
		return nil, ""
	}
	c.cmu.Lock()
	defer c.cmu.Unlock()
	for _, call := range c.activeCalls {
		if call != nil && call.cc != nil && call.cc.SIPCallID() == callID {
			return call, "call-id"
		}
	}
	return nil, ""
}

func requestCallID(req *sip.Request) string {
	if req == nil {
		return ""
	}
	if h := req.CallID(); h != nil {
		return h.Value()
	}
	return ""
}

func requestFromTag(req *sip.Request) string {
	if req == nil {
		return ""
	}
	from := req.From()
	if from == nil {
		return ""
	}
	tag, ok := getTagFrom(from.Params)
	if !ok {
		return ""
	}
	return string(tag)
}

func requestToTag(req *sip.Request) string {
	if req == nil {
		return ""
	}
	to := req.To()
	if to == nil {
		return ""
	}
	tag, ok := getTagFrom(to.Params)
	if !ok {
		return ""
	}
	return string(tag)
}

func requestHeaderValue(req *sip.Request, name string) string {
	if req == nil {
		return ""
	}
	h := req.GetHeader(name)
	if h == nil {
		return ""
	}
	return h.Value()
}

func (c *Client) onBye(req *sip.Request, tx sip.ServerTransaction) bool {
	ctx := context.Background()
	ctx, span := Tracer.Start(ctx, "sip.Client.onBye")
	defer span.End()
	call, matchedBy := c.getActiveCallForRequest(req)
	if call == nil {
		c.log.Warnw("BYE from remote did not match an active outbound call", nil,
			"callID", requestCallID(req),
			"fromTag", requestFromTag(req),
			"toTag", requestToTag(req),
			"reason", requestHeaderValue(req, "Reason"),
		)
		return false
	}
	reason, rawReason := outboundByeReason(req)
	call.log.Infow("BYE from remote", "matchedBy", matchedBy, "reason", reason.String(), "reason-raw", rawReason)
	go func(call *outboundCall) {
		call.cc.AcceptBye(req, tx)
		status, term, disconnectReason, sipStatus := classifyOutboundBye(reason)
		if sipStatus != nil {
			call.setFailureStatusAttrs(sipStatus)
		}
		call.CloseWithReason(ctx, status, term, disconnectReason)
	}(call)
	return true
}

func outboundByeReason(req *sip.Request) (ReasonHeader, string) {
	h := req.GetHeader("Reason")
	if h == nil {
		return ReasonHeader{}, ""
	}
	raw := h.Value()
	reason, err := ParseReasonHeader(raw)
	if err != nil {
		return ReasonHeader{}, raw
	}
	return reason, raw
}

func classifyOutboundBye(reason ReasonHeader) (CallStatus, stats.Termination, livekit.DisconnectReason, *livekit.SIPStatus) {
	if reason.IsNormal() {
		return CallHangup, stats.Success("bye"), livekit.DisconnectReason_CLIENT_INITIATED, nil
	}

	sipStatus := sipStatusFromByeReason(reason)
	if sipStatus == nil {
		term := stats.ClientError(fmt.Sprintf("bye-%s-%d", strings.ToLower(reason.Type), reason.Cause))
		return callRejected, term, livekit.DisconnectReason_USER_REJECTED, nil
	}

	res := classifyInviteError(fmt.Errorf("BYE reason: %w", sipStatus))
	return res.status, res.term, res.reason, sipStatus
}

func sipStatusFromByeReason(reason ReasonHeader) *livekit.SIPStatus {
	code := 0
	status := reason.Text
	switch reason.Type {
	case "sip":
		code = reason.Cause
	case "q.850":
		switch reason.Cause {
		case 1:
			code, status = int(sip.StatusNotFound), defaultSIPStatusText(sip.StatusNotFound, status)
		case 17:
			code, status = int(sip.StatusBusyHere), defaultSIPStatusText(sip.StatusBusyHere, status)
		case 18, 19, 20:
			code, status = int(sip.StatusTemporarilyUnavailable), defaultSIPStatusText(sip.StatusTemporarilyUnavailable, status)
		case 21:
			code, status = int(sip.StatusGlobalDecline), defaultSIPStatusText(sip.StatusGlobalDecline, status)
		case 34, 41, 42:
			code, status = int(sip.StatusServiceUnavailable), defaultSIPStatusText(sip.StatusServiceUnavailable, status)
		default:
			return nil
		}
	default:
		return nil
	}
	if code < 100 || code > 699 {
		return nil
	}
	return &livekit.SIPStatus{
		Code:   livekit.SIPStatusCode(code),
		Status: defaultSIPStatusText(sip.StatusCode(code), status),
	}
}

func defaultSIPStatusText(code sip.StatusCode, current string) string {
	if strings.TrimSpace(current) != "" {
		return current
	}
	switch code {
	case sip.StatusNotFound:
		return "Not Found"
	case sip.StatusTemporarilyUnavailable:
		return "Temporarily Unavailable"
	case sip.StatusBusyHere:
		return "Busy Here"
	case sip.StatusServiceUnavailable:
		return "Service Unavailable"
	case sip.StatusGlobalDecline:
		return "Declined"
	default:
		return sipStatus(code)
	}
}

func (c *Client) onNotify(req *sip.Request, tx sip.ServerTransaction) bool {
	call, _ := c.getActiveCallForRequest(req)
	if call == nil {
		return false
	}

	go func() {
		err := call.cc.handleNotify(req, tx)

		code, msg := sipCodeAndMessageFromError(err)

		tx.Respond(sip.NewResponseFromRequest(req, code, msg, nil))
	}()
	return true
}

func (c *Client) RegisterTransferSIPParticipant(sipCallID string, o *outboundCall) error {
	return c.handler.RegisterTransferSIPParticipantTopic(sipCallID)
}

func (c *Client) DeregisterTransferSIPParticipant(sipCallID string) {
	c.handler.DeregisterTransferSIPParticipantTopic(sipCallID)
}
