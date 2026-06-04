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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/frostbyte73/core"
	"github.com/icholy/digest"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"

	msdk "github.com/livekit/media-sdk"
	"github.com/livekit/media-sdk/dtmf"
	"github.com/livekit/media-sdk/sdp"
	"github.com/livekit/media-sdk/tones"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/livekit/protocol/utils/guid"
	"github.com/livekit/protocol/utils/traceid"
	"github.com/livekit/psrpc"
	lksdk "github.com/livekit/server-sdk-go/v2"
	"github.com/livekit/sipgo"
	"github.com/livekit/sipgo/sip"

	"github.com/livekit/sip/pkg/config"
	"github.com/livekit/sip/pkg/stats"
)

type sipOutboundConfig struct {
	address         string
	transport       livekit.SIPTransport
	host            string
	from            string
	to              string
	user            string
	pass            string
	dtmf            string
	dialtone        bool
	headers         map[string]string
	includeHeaders  livekit.SIPHeaderOptions
	headersToAttrs  map[string]string
	attrsToHeaders  map[string]string
	ringingTimeout  time.Duration
	maxCallDuration time.Duration
	enabledFeatures []livekit.SIPFeature
	featureFlags    map[string]string
	mediaConfig     *sipMediaConfig
	displayName     *string
	registerMode    outboundRegisterMode
	routeHeaders    []string
	registerDelay   time.Duration
}

const (
	inviteRetryAfterFreshRegisterDelay = 2 * time.Second
)

const (
	attrSIPInviteState      = livekit.AttrSIPPrefix + "inviteState"
	sipInviteStateCalling   = "calling"
	sipInviteStateEarly     = "early"
	sipInviteStateConfirmed = "confirmed"
)

type providerAuthConfigError struct {
	address  string
	fromUser string
	status   sip.StatusCode
}

func (e providerAuthConfigError) Error() string {
	return fmt.Sprintf("SIP IP trunk rejected INVITE auth with status %d for address %q and from user %q; verify public IP/port binding and outbound caller ID", e.status, e.address, e.fromUser)
}

func (e providerAuthConfigError) ClassifyInvite() inviteFailure {
	return inviteFailure{
		status:    callRejected,
		term:      stats.ClientError("provider-auth"),
		reason:    livekit.DisconnectReason_UNKNOWN_REASON,
		reportErr: nil,
		returnErr: psrpc.NewError(psrpc.FailedPrecondition, e),
	}
}

type outboundCall struct {
	c             *Client
	tid           traceid.ID
	log           logger.Logger
	state         *CallState
	regProfile    *ResolvedRegistrationConfig
	callStart     time.Time
	cc            *sipOutbound
	media         *MediaPort
	started       core.Fuse
	stopped       core.Fuse
	closing       core.Fuse
	stats         Stats
	sigTs         SignalingTimestamps
	jitterBuf     bool
	projectID     string
	directFrom    URI
	directContact URI

	mu       sync.RWMutex
	mon      *stats.CallMonitor
	lkRoom   RoomInterface
	lkRoomIn msdk.PCM16Writer // output to room; OPUS at 48k
	sipConf  sipOutboundConfig

	releaseTrunkSlot func()
}

func (c *Client) newCall(ctx context.Context, tid traceid.ID, conf *config.Config, log logger.Logger, id LocalTag, room RoomConfig, sipConf sipOutboundConfig, state *CallState, projectID string) (*outboundCall, error) {
	signalLoggingEnabled, _ := strconv.ParseBool(sipConf.featureFlags[signalLoggingFeatureFlag])
	if sipConf.maxCallDuration <= 0 || sipConf.maxCallDuration > maxCallDuration {
		sipConf.maxCallDuration = maxCallDuration
	}
	if sipConf.ringingTimeout <= 0 {
		sipConf.ringingTimeout = defaultRingingTimeout
	}
	jitterBuf := SelectValueBool(conf.EnableJitterBuffer, conf.EnableJitterBufferProb)
	room.JitterBuf = jitterBuf
	room.LogSignalChanges = signalLoggingEnabled

	tr := TransportFrom(sipConf.transport)
	contact := c.ContactURI(tr)
	defaultHost := sipConf.host
	if sipConf.host == "" {
		sipConf.host = contact.GetHost()
	}
	providerProfile := outboundProviderProfileForAddress(sipConf.address)
	sipConf.registerDelay = providerProfile.RegisterInviteSettlingDelay
	directContact := contact
	directFrom := URI{
		User:      sipConf.from,
		Host:      sipConf.host,
		Addr:      directContact.Addr,
		Transport: tr,
	}
	var regProfile *ResolvedRegistrationConfig
	if sipConf.registerMode != outboundRegisterModeDisabled && !(sipConf.registerMode == outboundRegisterModeAuto && providerProfile.SkipRegistrationInAuto) {
		var err error
		regSipConf := sipConf
		regSipConf.host = defaultHost
		regProfile, err = c.ensureRegistered(ctx, regSipConf)
		if sipConf.registerMode == outboundRegisterModeRequired && regProfile == nil && err == nil {
			err = psrpc.NewError(psrpc.InvalidArgument, errors.New("sip registration requires address, username, and password"))
		}
		if err != nil {
			if sipConf.registerMode == outboundRegisterModeRequired || regProfile == nil || !regProfile.InviteOnRegisterFailure {
				return nil, err
			}
			log.Warnw("SIP registration attempt failed, continuing without registration", err,
				"address", sipConf.address,
				"transport", tr,
				"username", sipConf.user,
				"registrar", regProfile.Registrar.GetDest(),
			)
			regProfile = nil
		}
	}
	if regProfile == nil && providerProfile.RouteRegisteredInviteToRegistrar && !providerProfile.DisableRegistrationCache {
		regProfile = c.cachedRegisteredRouteProfile(ctx, sipConf, defaultHost, tr, log)
	}
	if regProfile != nil {
		if regProfile.FromDomain != "" && defaultHost == "" {
			sipConf.host = regProfile.FromDomain
		}
		if regProfile.ContactUser != "" {
			contact.User = regProfile.ContactUser
		}
	}
	fromURI := URI{
		User:      sipConf.from,
		Host:      sipConf.host,
		Addr:      contact.Addr,
		Transport: tr,
	}
	if regProfile != nil {
		fromURI = URI{
			User: sipConf.from,
			Host: sipConf.host,
		}
	}
	now := time.Now()
	call := &outboundCall{
		c:             c,
		tid:           tid,
		log:           log,
		sipConf:       sipConf,
		state:         state,
		regProfile:    regProfile,
		callStart:     now,
		sigTs:         SignalingTimestamps{APITime: now},
		jitterBuf:     jitterBuf,
		projectID:     projectID,
		directFrom:    directFrom,
		directContact: directContact,
	}
	call.stats.Update()
	call.cc = c.newOutbound(log, id, fromURI, contact, sipConf.displayName, call.setAttrsToHeaders)
	call.cc.configuredRouteHeaders = cloneRouteHeaders(sipConf.routeHeaders)
	call.cc.routeRegisteredInviteToRegistrar = providerProfile.RouteRegisteredInviteToRegistrar
	call.log = call.log.WithValues("jitterBuf", call.jitterBuf, "sipCallID", call.cc.callID)
	call.cc.directProviderAuthConfigError = providerProfile.DirectAuthFailureIsConfigError &&
		(sipConf.registerMode == outboundRegisterModeDisabled || sipConf.registerMode == outboundRegisterModeAuto)
	if regProfile != nil {
		call.cc.routeHeaders = registeredInviteRouteHeaders(sipConf.routeHeaders, regProfile, providerProfile.RouteRegisteredInviteToRegistrar)
	} else {
		call.cc.routeHeaders = cloneRouteHeaders(sipConf.routeHeaders)
	}

	call.mon = c.mon.NewCall(stats.Outbound, sipConf.host, sipConf.address)

	var err error
	call.media, err = NewMediaPort(tid, call.log, call.mon, &MediaOptions{
		IP:                   c.sconf.MediaIP,
		BindIP:               c.sconf.SignalingIPLocal,
		Ports:                conf.RTPPort,
		MediaTimeoutInitial:  c.conf.MediaTimeoutInitial,
		MediaTimeout:         sipConf.mediaConfig.MediaTimeout,
		SymmetricRTP:         c.conf.SymmetricRTP,
		IgnoreLocalAddrInSDP: c.conf.IgnoreLocalAddrInSDP,
		EnableJitterBuffer:   call.jitterBuf,
		LogSignalChanges:     signalLoggingEnabled,
		Stats:                &call.stats.Port,
		NoInputResample:      !RoomResample,
		IgnorePreanswerData:  true,
	}, RoomSampleRate)
	if err != nil {
		call.close(ctx, errors.Wrap(err, "media failed"), callDropped, stats.ServerError("media-failed"), livekit.DisconnectReason_UNKNOWN_REASON)
		return nil, err
	}
	call.media.SetDTMFAudio(conf.AudioDTMF)
	call.media.EnableTimeout(false)
	call.media.DisableOut() // disabled until we get 200
	if err := call.connectToRoom(ctx, room, c.getRoom); err != nil {
		call.close(ctx, errors.Wrap(err, "room join failed"), callDropped, stats.ServerError("join-failed"), livekit.DisconnectReason_UNKNOWN_REASON)
		return nil, psrpc.NewError(psrpc.Internal, fmt.Errorf("update room failed: %w", err))
	}

	c.cmu.Lock()
	defer c.cmu.Unlock()
	c.activeCalls[id] = call
	return call, nil
}

func (c *Client) cachedRegisteredRouteProfile(ctx context.Context, sipConf sipOutboundConfig, defaultHost string, tr Transport, log logger.Logger) *ResolvedRegistrationConfig {
	cachedSipConf := sipConf
	cachedSipConf.host = defaultHost
	cachedSipConf.pass = ""
	if cachedSipConf.user == "" {
		cachedSipConf.user = sipConf.from
	}
	regProfile, err := c.ensureRegistered(ctx, cachedSipConf)
	if err != nil {
		log.Warnw("SIP registered route cache lookup failed", err,
			"address", sipConf.address,
			"transport", tr,
			"username", cachedSipConf.user,
		)
		return nil
	}
	if regProfile != nil {
		log.Infow("SIP registered route cache hit",
			"address", sipConf.address,
			"transport", tr,
			"username", cachedSipConf.user,
			"serviceRoutes", regProfile.ServiceRouteHeaders,
		)
	}
	return regProfile
}

func (c *outboundCall) setAttrsToHeaders(headers map[string]string) map[string]string {
	if len(c.sipConf.attrsToHeaders) == 0 {
		return headers
	}
	r := c.lkRoom.Room()
	if r == nil {
		return headers
	}
	return AttrsToHeaders(r.LocalParticipant.Attributes(), c.sipConf.attrsToHeaders, headers)
}

func (c *outboundCall) ensureClosed(ctx context.Context) {
	c.state.Update(ctx, func(info *livekit.SIPCallInfo) {
		if info.Error != "" {
			info.CallStatus = livekit.SIPCallStatus_SCS_ERROR
		} else {
			info.CallStatus = livekit.SIPCallStatus_SCS_DISCONNECTED
		}
		if r := c.lkRoom.Room(); r != nil {
			if p := r.LocalParticipant; p != nil {
				info.ParticipantIdentity = p.Identity()
				info.ParticipantAttributes = p.Attributes()
			}
		}
		info.EndedAtNs = time.Now().UnixNano()
	})
}

func (c *outboundCall) setErrStatus(ctx context.Context, err error) {
	if err == nil {
		return
	}
	c.state.Update(ctx, func(info *livekit.SIPCallInfo) {
		if info.Error != "" {
			return
		}
		info.Error = err.Error()
		info.CallStatus = livekit.SIPCallStatus_SCS_ERROR
	})
}

func (c *outboundCall) Dial(ctx context.Context) error {
	ctx, span := Tracer.Start(ctx, "sip.outbound.Dial")
	defer span.End()
	ctx, cancel := context.WithTimeout(ctx, c.sipConf.maxCallDuration)
	defer cancel()
	c.mon.CallStart()
	defer c.mon.CallEnd()

	err := c.connectSIP(ctx, c.tid)
	if err != nil {
		c.ensureClosed(ctx)
		return err // connectSIP updates the error code on the callInfo
	}

	c.state.Update(ctx, func(info *livekit.SIPCallInfo) {
		lkroom := c.lkRoom.Room()
		if lkroom == nil {
			c.log.Errorw("failed to update SIP info", fmt.Errorf("unexpected state: lkroom is not set"))
			return
		}
		info.RoomId = lkroom.SID()
		info.StartedAtNs = time.Now().UnixNano()
		info.CallStatus = livekit.SIPCallStatus_SCS_ACTIVE
	})
	return nil
}

func (c *outboundCall) WaitClose(ctx context.Context) error {
	ctx, span := Tracer.Start(ctx, "sip.outbound.WaitClose")
	defer span.End()
	return c.waitClose(ctx, c.tid)
}

func (c *outboundCall) waitClose(ctx context.Context, tid traceid.ID) error {
	ctx = context.WithoutCancel(ctx)
	defer c.ensureClosed(ctx)

	ticker := time.NewTicker(stateUpdateTick)
	defer ticker.Stop()

	statsTicker := time.NewTicker(statsInterval)
	defer statsTicker.Stop()
	for {
		select {
		case <-statsTicker.C:
			c.stats.Update()
			c.printStats()
		case <-ticker.C:
			c.log.Debugw("sending keep-alive")
			c.state.ForceFlush(ctx)
		case <-c.Disconnected():
			term := terminationFromRoomDisconnect(c.lkRoom.ClosedReason())
			c.CloseWithReason(ctx, callDropped, term, livekit.DisconnectReason_CLIENT_INITIATED)
			return nil
		case <-c.media.Timeout():
			c.closeWithTimeout(ctx)
			err := psrpc.NewErrorf(psrpc.DeadlineExceeded, "media timeout")
			c.setErrStatus(ctx, err)
			return err
		case <-c.Closed():
			return nil
		}
	}
}

func (c *outboundCall) DialAsync(ctx context.Context) {
	ctx, span := Tracer.Start(ctx, "sip.outbound.DialAsync")
	defer span.End()
	ctx = context.WithoutCancel(ctx)
	go func() {
		if err := c.Dial(ctx); err != nil {
			return
		}
		_ = c.WaitClose(ctx)
	}()
}

func (c *outboundCall) Closed() <-chan struct{} {
	return c.stopped.Watch()
}

func (c *outboundCall) Disconnected() <-chan struct{} {
	return c.lkRoom.Closed()
}

func (c *outboundCall) Close(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.close(ctx, nil, callDropped, stats.ServerError("shutdown"), livekit.DisconnectReason_SERVER_SHUTDOWN)
	return nil
}

func (c *outboundCall) CloseWithReason(ctx context.Context, status CallStatus, t stats.Termination, reason livekit.DisconnectReason) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.close(ctx, nil, status, t, reason)
}

func (c *outboundCall) closeWithTimeout(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.close(ctx, psrpc.NewErrorf(psrpc.DeadlineExceeded, "media-timeout"), callDropped, stats.ServerError("media-timeout"), livekit.DisconnectReason_UNKNOWN_REASON)
}

func (c *outboundCall) printStats() {
	c.stats.Log(c.log, c.callStart)
}

func (c *outboundCall) close(ctx context.Context, err error, status CallStatus, t stats.Termination, reason livekit.DisconnectReason) {
	c.closing.Break()
	ctx = context.WithoutCancel(ctx)
	c.stopped.Once(func() {
		c.stats.Closed.Store(true)
		log := c.log.WithValues("status", status, "result", string(t.Result), "reason", t.Reason)
		defer func() {
			if c.releaseTrunkSlot != nil {
				c.releaseTrunkSlot()
			}
			c.stats.Update()
			c.printStats()
			c.sigTs.Log(log)
		}()

		c.setStatus(status)
		if err != nil {
			log.Warnw("Closing outbound call with error", nil)
		} else {
			log.Infow("Closing outbound call")
		}
		c.state.Update(ctx, func(info *livekit.SIPCallInfo) {
			if err != nil && info.Error == "" {
				info.Error = err.Error()
				info.CallStatus = livekit.SIPCallStatus_SCS_ERROR
			}
			info.DisconnectReason = reason
		})

		// Send BYE _before_ closing media/room connection.
		// This ensures participant attributes are still available for
		// attributes_to_headers mapping in the setHeaders callback.
		// See: https://github.com/livekit/sip/issues/404
		c.stopSIP(ctx, t)
		if c.media != nil {
			c.media.Close()
		}

		if r := c.lkRoom; r != nil {
			_ = r.CloseOutput()
			_ = r.CloseWithReason(status.DisconnectReason())
		}
		c.lkRoomIn = nil

		c.c.cmu.Lock()
		delete(c.c.activeCalls, c.cc.ID())
		c.c.cmu.Unlock()

		c.c.DeregisterTransferSIPParticipant(string(c.cc.ID()))

		// Call the handler asynchronously to avoid blocking
		if c.c.handler != nil {
			go func(tid traceid.ID) {
				ctx := context.WithoutCancel(ctx)
				ctx, span := Tracer.Start(ctx, "sip.outbound.OnSessionEnd")
				defer span.End()
				c.c.handler.OnSessionEnd(ctx, &CallIdentifier{
					TraceID:   tid,
					ProjectID: c.projectID,
					CallID:    c.state.callInfo.CallId,
					SipCallID: c.cc.SIPCallID(),
				}, c.state.callInfo, t.Reason)
			}(c.tid)
		}
	})
}

func (c *outboundCall) Participant() ParticipantInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lkRoom.Participant()
}

func (c *outboundCall) connectSIP(ctx context.Context, tid traceid.ID) error {
	ctx, span := Tracer.Start(ctx, "sip.outbound.connectSIP")
	defer span.End()
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.dialSIP(ctx, tid); err != nil {
		c.log.Infow("SIP call failed", "error", err)
		res := classifyInviteError(err)
		c.close(ctx, res.reportErr, res.status, res.term, res.reason)
		return res.returnErr
	}
	c.connectMedia()
	c.started.Break()
	c.lkRoom.Subscribe()
	c.log.Infow("Outbound SIP call established")
	return nil
}

func (c *outboundCall) classifySIPConnectError(err error) (error, CallStatus, stats.Termination, livekit.DisconnectReason) {
	reportErr := err
	status, term, reason := callDropped, stats.ServerError("invite-failed"), livekit.DisconnectReason_UNKNOWN_REASON
	var e *livekit.SIPStatus
	if errors.As(err, &e) {
		switch int(e.Code) {
		case int(sip.StatusForbidden):
			status, term, reason = callDropped, stats.ServerError("forbidden"), livekit.DisconnectReason_SIP_TRUNK_FAILURE
		case int(sip.StatusNotFound):
			status, term, reason = callUnavailable, stats.ClientError("not-found"), livekit.DisconnectReason_USER_UNAVAILABLE
			reportErr = nil
		case int(sip.StatusRequestTerminated):
			status, term, reason = callRejected, stats.ClientError("request-terminated"), livekit.DisconnectReason_USER_REJECTED
			reportErr = nil
		case int(sip.StatusTemporarilyUnavailable):
			status, term, reason = callUnavailable, stats.ClientError("unavailable"), livekit.DisconnectReason_USER_UNAVAILABLE
			reportErr = nil
		case int(sip.StatusRequestTimeout):
			status, term, reason = callUnavailable, stats.ClientError("request-timeout"), livekit.DisconnectReason_USER_UNAVAILABLE
			reportErr = nil
		case int(sip.StatusBusyHere):
			status, term, reason = callRejected, stats.ClientError("busy"), livekit.DisconnectReason_USER_REJECTED
			reportErr = nil
		case int(sip.StatusGlobalDecline):
			status, term, reason = callRejected, stats.ClientError("declined"), livekit.DisconnectReason_USER_REJECTED
			reportErr = nil
		case int(sip.StatusInternalServerError):
			status, term, reason = callDropped, stats.ServerError("internal-server-error"), livekit.DisconnectReason_SIP_TRUNK_FAILURE
		case int(sip.StatusBadGateway):
			status, term, reason = callDropped, stats.ServerError("bad-gateway"), livekit.DisconnectReason_SIP_TRUNK_FAILURE
		case int(sip.StatusServiceUnavailable):
			status, term, reason = callDropped, stats.ServerError("service-unavailable"), livekit.DisconnectReason_SIP_TRUNK_FAILURE
		default:
			if e.Code >= 500 && e.Code < 600 {
				status, term, reason = callDropped, stats.ServerError("sip-5xx"), livekit.DisconnectReason_SIP_TRUNK_FAILURE
			}
		}
	} else if e := (SDPError{}); errors.As(err, &e) {
		status, reason = callRejected, livekit.DisconnectReason_MEDIA_FAILURE
		reportErr = nil
		err = psrpc.NewError(psrpc.FailedPrecondition, e.Err)
		if errors.Is(e.Err, sdp.ErrNoCommonMedia) {
			term = stats.ClientError("no-common-codec")
		} else if errors.Is(e.Err, sdp.ErrNoCommonCrypto) {
			term = stats.ClientError("encryption-required")
		} else {
			term = stats.ClientError("sdp-error")
		}
	} else if e := (providerAuthConfigError{}); errors.As(err, &e) {
		status, term, reason = callRejected, stats.ClientError("provider-auth"), livekit.DisconnectReason_UNKNOWN_REASON
		reportErr = nil
	} else if isOutboundInviteAuthRetryExhausted(err) {
		status, term, reason = callDropped, stats.ServerError("auth-retry-exhausted"), livekit.DisconnectReason_SIP_TRUNK_FAILURE
	} else if c.inviteTimedOut(err) {
		if c.hasRemoteInviteProgress() {
			status, term, reason = callUnavailable, stats.ClientError("request-timeout"), livekit.DisconnectReason_USER_UNAVAILABLE
			reportErr = nil
		} else {
			status, term, reason = callDropped, stats.ServerError("request-timeout"), livekit.DisconnectReason_SIP_TRUNK_FAILURE
		}
	}
	return reportErr, status, term, reason
}

func isOutboundInviteAuthRetryExhausted(err error) bool {
	return strings.Contains(err.Error(), "max auth retry attempts reached for SIP invite")
}

func (c *outboundCall) inviteTimedOut(err error) bool {
	var psrpcErr psrpc.Error
	return errors.As(err, &psrpcErr) &&
		psrpcErr.Code() == psrpc.Canceled &&
		strings.Contains(err.Error(), "sip request timed out")
}

func (c *outboundCall) hasRemoteInviteProgress() bool {
	return c != nil && (!c.sigTs.TryingTime.IsZero() || !c.sigTs.RingingTime.IsZero())
}

func (c *outboundCall) connectToRoom(ctx context.Context, lkNew RoomConfig, getRoom GetRoomFunc) error {
	ctx, span := Tracer.Start(ctx, "sip.outbound.connectToRoom")
	defer span.End()
	attrs := lkNew.Participant.Attributes
	if attrs == nil {
		attrs = make(map[string]string)
	}

	sipCallID := attrs[livekit.AttrSIPCallID]
	if sipCallID != "" {
		c.c.RegisterTransferSIPParticipant(sipCallID, c)
	}

	attrs[livekit.AttrSIPCallStatus] = CallDialing.Attribute()
	attrs[attrSIPInviteState] = sipInviteStateCalling
	lkNew.Participant.Attributes = attrs
	r := getRoom(c.log, &c.stats.Room)
	if err := r.Connect(ctx, c.c.conf, lkNew); err != nil {
		_ = r.Close()
		return err
	}
	// We have to create the track early because we might play a dialtone while SIP connects.
	// Thus, we are forced to set full sample rate here instead of letting the codec adapt to the SIP source sample rate.
	local, err := r.NewParticipantTrack(RoomSampleRate)
	if err != nil {
		_ = r.Close()
		return err
	}
	c.lkRoom = r
	c.lkRoomIn = local
	return nil
}

func (c *outboundCall) dialSIP(ctx context.Context, tid traceid.ID) error {
	if c.sipConf.dialtone {
		const ringVolume = math.MaxInt16 / 2
		rctx, rcancel := context.WithCancel(ctx)
		defer rcancel()

		dst := c.lkRoomIn // already under mutex

		// Play dialtone to the room while participant connects
		go func(tid traceid.ID) {
			rctx, span := Tracer.Start(rctx, "tones.Play")
			defer span.End()

			if dst == nil {
				c.log.Infow("room is not ready, ignoring dial tone")
				return
			}
			err := tones.Play(rctx, dst, ringVolume, tones.ETSIRinging)
			if err != nil && !errors.Is(err, context.Canceled) {
				c.log.Infow("cannot play dial tone", "error", err)
			}
		}(tid)
	}
	err := c.sipSignal(ctx, tid)
	if err != nil {
		return err
	}

	if digits := c.sipConf.dtmf; digits != "" {
		c.setStatus(CallAutomation)
		// Write initial DTMF to SIP
		if err := c.media.WriteDTMF(ctx, digits); err != nil {
			return err
		}
	}
	c.setStatus(CallActive)

	return nil
}

func (c *outboundCall) connectMedia() {
	if w := c.lkRoom.SwapOutput(c.media.GetAudioWriter()); w != nil {
		_ = w.Close()
	}
	c.lkRoom.SetDTMFOutput(c.media)

	c.media.WriteAudioTo(c.lkRoomIn)
	c.media.HandleDTMF(c.handleDTMF)
}

type sipRespFunc func(code sip.StatusCode, hdrs Headers)
type sipInviteSentFunc func()

func sipResponse(ctx context.Context, tx sip.ClientTransaction, stop <-chan struct{}, setState sipRespFunc) (*sip.Response, error) {
	cnt := 0
	for {
		select {
		case <-ctx.Done():
			_ = tx.Cancel()
			// NOTE: psrpc.Canceled does not auto-retry, whereas psrpc.DeadlineExceeded does
			// As long as that is the case, avoid psrpc.DeadlineExceeded to prevent hammering of destination.
			return nil, fmt.Errorf("%w: %w", ErrSIPRequestTimeout, &livekit.SIPStatus{
				Code:   livekit.SIPStatusCode_SIP_STATUS_REQUEST_TIMEOUT,
				Status: "Request Timeout",
			})
		case <-stop:
			_ = tx.Cancel()
			return nil, psrpc.NewErrorf(psrpc.Canceled, "service shutting down")
		case <-tx.Done():
			return nil, psrpc.NewErrorf(psrpc.Canceled, "transaction failed to complete (%d intermediate responses)", cnt)
		case res, ok := <-tx.Responses():
			if !ok || res == nil {
				return nil, psrpc.NewErrorf(psrpc.Canceled, "transaction response channel closed (%d intermediate responses)", cnt)
			}
			status := res.StatusCode
			if setState != nil {
				setState(res.StatusCode, res.Headers())
			}
			if status/100 != 1 { // != 1xx
				return res, nil
			}
			// continue
			cnt++
		}
	}
}

func (c *outboundCall) stopSIP(ctx context.Context, t stats.Termination) {
	termCtx, cancel := context.WithCancel(context.Background()) // Do not use ctx
	defer cancel()
	go func() {
		select {
		case <-termCtx.Done():
			return
		case <-time.After(5 * time.Minute):
			c.mon.CallTerminationFailure()
			c.log.Errorw("call failed to terminate after 5 minutes", nil) // To be able to get call IDs
		}
	}()

	c.mon.CallTerminate(t)
	c.cc.Close(ctx)
}

func (c *outboundCall) setStatus(v CallStatus) {
	attr := v.Attribute()
	if attr == "" {
		return
	}
	if c.lkRoom == nil {
		return
	}
	r := c.lkRoom.Room()
	if r == nil {
		return
	}
	r.LocalParticipant.SetAttributes(map[string]string{
		livekit.AttrSIPCallStatus: attr,
	})
}

func (c *outboundCall) setInviteState(v string) {
	if v == "" {
		return
	}
	if c.lkRoom == nil {
		return
	}
	r := c.lkRoom.Room()
	if r == nil {
		return
	}
	r.LocalParticipant.SetAttributes(map[string]string{
		attrSIPInviteState: v,
	})
}

func (c *outboundCall) setFailureStatusAttrs(status *livekit.SIPStatus) {
	if status == nil || c.lkRoom == nil {
		return
	}
	r := c.lkRoom.Room()
	if r == nil {
		return
	}
	attrs := map[string]string{
		livekit.AttrSIPCallStatus: "dialing-error",
		"sip.statusCode":          strconv.Itoa(int(status.Code)),
	}
	if status.Status != "" {
		attrs["sip.status"] = status.Status
		attrs["sip.reason"] = status.Status
	}
	r.LocalParticipant.SetAttributes(attrs)
}

func (c *outboundCall) setExtraAttrs(hdrToAttr map[string]string, opts livekit.SIPHeaderOptions, cc Signaling, hdrs Headers) {
	extra := HeadersToAttrs(nil, hdrToAttr, opts, cc, hdrs)
	if c.lkRoom != nil && len(extra) != 0 {
		room := c.lkRoom.Room()
		if room != nil {
			room.LocalParticipant.SetAttributes(extra)
		} else {
			c.log.Warnw("could not set attributes on nil room", nil, "attrs", extra)
		}
	}
}

func (c *outboundCall) sipSignal(ctx context.Context, tid traceid.ID) error {
	ctx, span := Tracer.Start(ctx, "sip.outbound.sipSignal")
	defer span.End()

	if c.sipConf.ringingTimeout > 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, c.sipConf.ringingTimeout)
		defer cancel()
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-ctx.Done():
			// parent context cancellation or success
			return
		case <-c.Disconnected():
		case <-c.Closed():
		}
		cancel()
	}()

	mconf := c.sipConf.mediaConfig
	sdpOffer, err := c.media.NewOffer(mconf.Codecs, mconf.Encryption)
	if err != nil {
		return err
	}
	sdpOfferData, err := sdpOffer.SDP.Marshal()
	if err != nil {
		return err
	}
	c.mon.SDPSize(len(sdpOfferData), true)
	c.log.Debugw("SDP offer", "sdp", string(sdpOfferData))
	joinDur := c.mon.JoinDur()

	c.mon.InviteReq()
	c.sigTs.InviteTime = time.Now()

	toUri := CreateURIFromUserAndAddress(c.sipConf.to, c.sipConf.address, TransportFrom(c.sipConf.transport))
	if c.regProfile != nil {
		delay := c.sipConf.registerDelay
		if delay <= 0 {
			delay = invitePostRegisterSettlingDelay
		}
		if err := c.c.registrationManager.waitForRegisterSettling(ctx, c.regProfile.cacheKey(), delay); err != nil {
			return err
		}
	}

	ringing := false
	setState := func(code sip.StatusCode, hdrs Headers) {
		if code == sip.StatusOK {
			return // is set separately
		}
		if code == sip.StatusTrying && c.sigTs.TryingTime.IsZero() {
			c.sigTs.TryingTime = time.Now()
		}
		if !ringing && code >= sip.StatusRinging && code < sip.StatusOK {
			ringing = true
			c.sigTs.RingingTime = time.Now()
			c.setStatus(CallRinging)
			c.setInviteState(sipInviteStateEarly)
		}
		c.setExtraAttrs(nil, 0, nil, hdrs)
	}
	sdpResp, err := c.cc.Invite(ctx, toUri, c.regProfile, c.sipConf.user, c.sipConf.pass, c.sipConf.headers, sdpOfferData, setState, c.releaseTrunkSlot)
	if c.shouldRetryInviteWithoutRegistration(err) {
		sdpResp, err = c.retryInviteWithoutRegistration(ctx, toUri, sdpOfferData, setState, c.releaseTrunkSlot)
	}
	// Update SIPCallInfo with the SIP Call-ID after Invite
	if sipCallID := c.cc.SIPCallID(); sipCallID != "" {
		c.state.DeferUpdate(func(info *livekit.SIPCallInfo) {
			info.SipCallId = sipCallID
			// Set callidfull in participant attributes for backwards compatibility
			if info.ParticipantAttributes == nil {
				info.ParticipantAttributes = make(map[string]string)
			}
			info.ParticipantAttributes[AttrSIPCallIDFull] = sipCallID
		})
	}
	if err != nil {
		// TODO: should we retry? maybe new offer will work
		var e *livekit.SIPStatus
		if errors.As(err, &e) {
			c.mon.InviteError(statusName(int(e.Code)))
			c.state.DeferUpdate(func(info *livekit.SIPCallInfo) {
				info.CallStatusCode = e
			})
			c.setFailureStatusAttrs(e)
			c.log.Infow("SIP invite rejected",
				"status", e.Code,
				"reason", e.Status,
				"sdpOffer", string(sdpOfferData),
			)
		} else {
			c.mon.InviteError("other")
			c.log.Infow("SIP invite failed without SIP status",
				"error", err,
				"sdpOffer", string(sdpOfferData),
			)
		}
		c.cc.Close(ctx)
		c.log.Infow("SIP invite failed", "error", err)
		return err
	}
	c.sigTs.AcceptTime = time.Now()
	c.mon.SDPSize(len(sdpResp), false)
	c.log.Debugw("SDP answer", "sdp", string(sdpResp))

	c.log = LoggerWithHeaders(c.log, c.cc)

	mc, localSDP, err := c.media.SetAnswer(sdpOffer, sdpResp, mconf.Codecs, mconf.Encryption)
	if err != nil {
		return err
	}
	if err = c.media.SetConfig(mc); err != nil {
		return err
	}
	mc.Processor = c.c.handler.GetMediaProcessor(c.sipConf.enabledFeatures, c.sipConf.featureFlags, string(c.cc.ID()), MediaProcessorOpts{InputSampleRate: c.media.InputSampleRate()})
	c.cc.SetLocalSDP(localSDP)

	c.mon.InviteAccept()
	c.media.EnableOut()
	c.media.EnableTimeout(true)
	err = c.cc.AckInviteOK(ctx)
	if err != nil {
		c.log.Infow("SIP accept failed", "error", err)
		return err
	}
	c.sigTs.AckTime = time.Now()
	c.setInviteState(sipInviteStateConfirmed)
	joinDur()

	c.setExtraAttrs(c.sipConf.headersToAttrs, c.sipConf.includeHeaders, c.cc, nil)
	c.state.DeferUpdate(func(info *livekit.SIPCallInfo) {
		info.AudioCodec = mc.Audio.Codec.Info().SDPName
		if r := c.lkRoom.Room(); r != nil {
			info.ParticipantAttributes = r.LocalParticipant.Attributes()
		}
	})
	return nil
}

func (c *outboundCall) shouldRetryInviteWithoutRegistration(err error) bool {
	if c == nil || c.regProfile == nil || c.sipConf.registerMode != outboundRegisterModeAuto {
		return false
	}
	if !outboundProviderProfileForAddress(c.sipConf.address).AllowRegisteredInviteDirectFallback {
		return false
	}
	var sipErr *livekit.SIPStatus
	return errors.As(err, &sipErr) && sipErr.Code == livekit.SIPStatusCode_SIP_STATUS_BUSY_HERE
}

func (c *outboundCall) retryInviteWithoutRegistration(ctx context.Context, toUri URI, sdpOfferData []byte, setState sipRespFunc, onInviteSent sipInviteSentFunc) ([]byte, error) {
	c.log.Infow("retrying SIP INVITE without outbound REGISTER profile",
		"retry_reason", "registered_invite_busy_here",
		"registerMode", c.sipConf.registerMode.String(),
	)
	getHeaders := c.cc.getHeaders
	c.cc.Close(ctx)
	c.cc = c.c.newOutbound(c.log, c.cc.ID(), c.directFrom, c.directContact, c.sipConf.displayName, getHeaders)
	c.regProfile = nil
	c.mon.InviteReq()
	return c.cc.InviteWithFreshCallID(ctx, toUri, nil, c.sipConf.user, c.sipConf.pass, c.sipConf.headers, sdpOfferData, setState, onInviteSent)
}

func (c *outboundCall) handleDTMF(ev dtmf.Event) {
	if c.lkRoom == nil {
		return
	}
	_ = c.lkRoom.SendData(&livekit.SipDTMF{
		Code:  uint32(ev.Code),
		Digit: string([]byte{ev.Digit}),
	}, lksdk.WithDataPublishReliable(true))
}

func (c *outboundCall) transferCall(ctx context.Context, transferTo string, headers map[string]string, dialtone bool) (retErr error) {
	ctx, span := Tracer.Start(ctx, "sip.outbound.transferCall")
	defer span.End()
	var err error

	tID := c.state.StartTransfer(ctx, transferTo)
	defer func() {
		c.state.EndTransfer(ctx, tID, retErr)
	}()

	if dialtone && c.started.IsBroken() && !c.stopped.IsBroken() {
		const ringVolume = math.MaxInt16 / 2
		rctx, rcancel := context.WithCancel(ctx)
		defer rcancel()

		// mute the room audio to the SIP participant
		w := c.lkRoom.SwapOutput(nil)

		defer func() {
			if retErr != nil && !c.stopped.IsBroken() {
				c.lkRoom.SwapOutput(w)
			} else {
				w.Close()
			}
		}()

		go func() {
			aw := c.media.GetAudioWriter()

			err := tones.Play(rctx, aw, ringVolume, tones.ETSIRinging)
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				c.log.Infow("cannot play dial tone", "error", err)
			}
		}()
	}

	err = c.cc.transferCall(ctx, transferTo, headers, c.closing.Watch())
	if err != nil {
		c.log.Infow("outbound call failed to transfer", "error", err, "transferTo", transferTo)
		return err
	}

	c.log.Infow("outbound call transferred", "transferTo", transferTo)

	// Give time for the peer to hang up first, but hang up ourselves if this doesn't happen within 1 second
	time.AfterFunc(referByeTimeout, func() {
		c.CloseWithReason(ctx, CallHangup, stats.Success("call transferred"), livekit.DisconnectReason_CLIENT_INITIATED)
	})

	return nil
}

func (c *Client) newOutbound(log logger.Logger, id LocalTag, from, contact URI, displayName *string, getHeaders setHeadersFunc) *sipOutbound {
	from = from.Normalize()
	if displayName == nil { // Nothing specified, preserve legacy behavior
		displayName = &from.User
	}

	fromHeader := &sip.FromHeader{
		DisplayName: *displayName,
		Address:     *from.GetURI(),
		Params:      sip.NewParams(),
	}
	contactHeader := &sip.ContactHeader{
		Address: *contact.GetContactURI(),
	}
	fromHeader.Params.Add("tag", string(id))
	return &sipOutbound{
		log:        log,
		c:          c,
		id:         id,
		callID:     guid.HashedID(string(id)),
		from:       fromHeader,
		contact:    contactHeader,
		referDone:  make(chan error), // Do not buffer the channel to avoid reading a result for an old request
		nextCSeq:   1,
		getHeaders: getHeaders,
	}
}

type sipOutbound struct {
	log                              logger.Logger
	c                                *Client
	id                               LocalTag
	from                             *sip.FromHeader
	contact                          *sip.ContactHeader
	routeHeaders                     []string
	configuredRouteHeaders           []string
	routeRegisteredInviteToRegistrar bool
	directProviderAuthConfigError    bool

	mu         sync.RWMutex
	tag        RemoteTag
	callID     string
	invite     *sip.Request
	inviteOk   *sip.Response
	localSDP   []byte // SDP Offer, constrained by the answer
	to         *sip.ToHeader
	nextCSeq   uint32
	getHeaders setHeadersFunc

	referCseq        uint32
	referDone        chan error
	latestInviteCSeq uint32
}

func (c *sipOutbound) From() sip.Uri {
	return c.from.Address
}

func (c *sipOutbound) To() sip.Uri {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.to == nil {
		return sip.Uri{}
	}
	return c.to.Address
}

func (c *sipOutbound) Address() sip.Uri {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.invite == nil {
		return sip.Uri{}
	}
	return c.invite.Recipient
}

func (c *sipOutbound) ID() LocalTag {
	return c.id
}

func (c *sipOutbound) Tag() RemoteTag {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.tag
}

func (c *sipOutbound) SIPCallID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.callID
}

func (c *sipOutbound) InviteCSeq() uint32 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.latestInviteCSeq
}

func (c *sipOutbound) RecordInvite(cseq uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cseq > c.latestInviteCSeq {
		c.latestInviteCSeq = cseq
	}
}

// SetLocalSDP stores the precomputed local SDP for re-INVITE (from ApplyWithLocal).
func (c *sipOutbound) SetLocalSDP(localSDP []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.localSDP = localSDP
}

// LocalSDP returns the precomputed local SDP for re-INVITE (from ApplyWithLocal).
func (c *sipOutbound) LocalSDP() []byte {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.localSDP
}

// Returns the original SDP offer.
func (c *sipOutbound) OwnSDP() []byte {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.invite == nil {
		return nil
	}
	body := c.invite.Body()
	if len(body) == 0 {
		return nil
	}
	out := make([]byte, len(body))
	copy(out, body)
	return out
}

func (c *sipOutbound) RemoteHeaders() Headers {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.inviteOk == nil {
		return nil
	}
	return c.inviteOk.Headers()
}

func (c *sipOutbound) Invite(ctx context.Context, to URI, regProfile *ResolvedRegistrationConfig, user, pass string, headers map[string]string, sdpOffer []byte, setState sipRespFunc, onInviteSent sipInviteSentFunc) ([]byte, error) {
	return c.doInvite(ctx, to, regProfile, user, pass, headers, sdpOffer, setState, onInviteSent, false)
}

func (c *sipOutbound) InviteWithFreshCallID(ctx context.Context, to URI, regProfile *ResolvedRegistrationConfig, user, pass string, headers map[string]string, sdpOffer []byte, setState sipRespFunc, onInviteSent sipInviteSentFunc) ([]byte, error) {
	return c.doInvite(ctx, to, regProfile, user, pass, headers, sdpOffer, setState, onInviteSent, true)
}

func (c *sipOutbound) doInvite(ctx context.Context, to URI, regProfile *ResolvedRegistrationConfig, user, pass string, headers map[string]string, sdpOffer []byte, setState sipRespFunc, onInviteSent sipInviteSentFunc, freshCallID bool) ([]byte, error) {
	ctx, span := Tracer.Start(ctx, "sip.outbound.Invite")
	defer span.End()
	c.mu.Lock()
	defer c.mu.Unlock()
	toHeader := &sip.ToHeader{Address: *to.GetURI()}

	if freshCallID {
		c.callID = guid.New("call_")
		c.nextCSeq = 1
	} else {
		c.callID = guid.HashedID(fmt.Sprintf("%s-%s", string(c.id), toHeader.Address.String()))
	}
	c.log = c.log.WithValues("sipCallID", c.callID)

	var (
		sipHeaders       Headers
		authHeaders      = make(map[string]string)
		req              *sip.Request
		resp             *sip.Response
		err              error
		lastAuthStatus   sip.StatusCode
		lastAuthAddress  string
		lastAuthFromUser string
	)
	if keys := maps.Keys(headers); len(keys) != 0 {
		sort.Strings(keys)
		for _, key := range keys {
			sipHeaders = append(sipHeaders, sip.NewHeader(key, headers[key]))
		}
	}
	inviteRetried := false
authLoop:
	for try := 0; ; try++ {
		if try >= inviteAuthMaxAttempts {
			if c.directProviderAuthConfigError && lastAuthStatus != 0 {
				return nil, psrpc.NewError(psrpc.FailedPrecondition, providerAuthConfigError{
					address:  lastAuthAddress,
					fromUser: lastAuthFromUser,
					status:   lastAuthStatus,
				})
			}
			return nil, psrpc.NewError(psrpc.FailedPrecondition, ErrAuthMaxRetry)
		}
		req, resp, err = c.attemptInvite(ctx, sip.CallIDHeader(c.callID), toHeader, sdpOffer, authHeaders, sipHeaders, setState, onInviteSent)
		if err != nil {
			return nil, err
		}
		var authHeaderName string
		var authHeaderRespName string
		switch resp.StatusCode {
		case sip.StatusOK:
			c.logInviteAcceptedResponse(resp)
			break authLoop
		default:
			c.logInviteFinalResponse(resp)
			return nil, fmt.Errorf("unexpected status from INVITE response: %w", &livekit.SIPStatus{
				Code:   livekit.SIPStatusCode(resp.StatusCode),
				Status: resp.Reason,
			})
		case sip.StatusBadRequest,
			sip.StatusForbidden,
			sip.StatusNotFound,
			sip.StatusRequestTerminated,
			sip.StatusTemporarilyUnavailable,
			sip.StatusServiceUnavailable,
			sip.StatusNotAcceptableHere,
			sip.StatusBusyHere:
			if !inviteRetried {
				retried, err := c.retryInviteAfterFreshRegister(ctx, regProfile, pass, resp)
				if err != nil {
					return nil, err
				}
				if retried {
					inviteRetried = true
					continue
				}
			}
			c.logInviteFinalResponse(resp)
			err := &livekit.SIPStatus{
				Code:   livekit.SIPStatusCode(resp.StatusCode),
				Status: resp.Reason,
			}
			if body := resp.Body(); len(body) != 0 {
				err.Status = string(body)
			} else if s := resp.GetHeader("X-Twilio-Error"); s != nil {
				err.Status = s.Value()
			}
			return nil, fmt.Errorf("INVITE failed: %w", err)
		case sip.StatusUnauthorized:
			authHeaderName = "WWW-Authenticate"
			authHeaderRespName = "Authorization"
		case sip.StatusProxyAuthRequired:
			authHeaderName = "Proxy-Authenticate"
			authHeaderRespName = "Proxy-Authorization"
		}
		c.log.Infow("auth requested", "status", resp.StatusCode, "body", string(resp.Body()))
		lastAuthStatus = resp.StatusCode
		lastAuthAddress = req.Recipient.Host
		lastAuthFromUser = c.from.Address.User
		// auth required
		if user == "" || pass == "" {
			return nil, psrpc.NewError(psrpc.FailedPrecondition, ErrAuthMissingCreds)
		}
		headerVal := resp.GetHeader(authHeaderName)
		if headerVal == nil {
			return nil, psrpc.NewError(psrpc.FailedPrecondition, ErrAuthNoHeader)
		}
		challengeStr := headerVal.Value()
		challenge, err := digest.ParseChallenge(challengeStr)
		if err != nil {
			return nil, psrpc.NewErrorf(psrpc.Internal, "invalid challenge %q: %v", challengeStr, err)
		}
		c.log.Infow("SIP INVITE auth challenge parsed", inviteAuthChallengeLogFields(resp.StatusCode, authHeaderName, authHeaderRespName, challenge)...)
		digestURI := req.Recipient.String()
		cred, err := digest.Digest(challenge, digest.Options{
			Method:   req.Method.String(),
			URI:      digestURI,
			Username: user,
			Password: pass,
		})
		if err != nil {
			return nil, err
		}
		c.log.Infow("SIP INVITE auth response prepared", inviteAuthResponseLogFields(authHeaderRespName, req, cred, digestURI)...)
		authHeaders[authHeaderRespName] = cred.String()
		// Try again with a computed digest
	}
	c.invite, c.inviteOk = req, resp
	toHeader = resp.To()
	if toHeader == nil {
		return nil, psrpc.NewErrorf(psrpc.Internal, "no To header in INVITE response")
	}
	var ok bool
	c.tag, ok = getTagFrom(toHeader.Params)
	if !ok {
		return nil, psrpc.NewErrorf(psrpc.Internal, "no tag in To header in INVITE response")
	}

	if cont := resp.Contact(); cont != nil {
		req.Recipient = cont.Address
		if req.Recipient.Port == 0 {
			req.Recipient.Port = 5060
		}
	}

	// We currently don't plumb the request back to caller to construct the ACK with.
	// Thus, we need to modify the request to update any route sets.
	for req.RemoveHeader("Route") {
	}
	for _, hdr := range resp.GetHeaders("Record-Route") {
		req.PrependHeader(&sip.RouteHeader{Address: hdr.(*sip.RecordRouteHeader).Address})
	}

	return c.inviteOk.Body(), nil
}

func (c *sipOutbound) retryInviteAfterFreshRegister(ctx context.Context, regProfile *ResolvedRegistrationConfig, password string, resp *sip.Response) (bool, error) {
	if c == nil || c.c == nil || c.c.registrationManager == nil || regProfile == nil || resp == nil {
		return false, nil
	}
	if !shouldRetryInviteAfterFreshRegister(resp) {
		return false, nil
	}
	forceRegisterRetry := serviceNotAuthorised(resp)
	age, ok := c.c.registrationManager.freshSuccessfulRegisterAge(regProfile.cacheKey(), freshRegisterInviteRetry)
	if !forceRegisterRetry && !ok {
		return false, nil
	}
	c.log.Infow("retrying SIP INVITE after fresh REGISTER",
		"retry_reason", "fresh_register_temporary_failure",
		"original_status", resp.StatusCode,
		"fresh_register_age_ms", age.Milliseconds(),
		"force_register_retry", forceRegisterRetry,
		"retry_attempt", 1,
	)
	timer := time.NewTimer(inviteRetryAfterFreshRegisterDelay)
	defer timer.Stop()
	select {
	case <-timer.C:
		c.log.Infow("forcing SIP REGISTER before INVITE retry",
			"retry_reason", "fresh_register_temporary_failure",
			"original_status", resp.StatusCode,
		)
		if err := c.c.forceRegister(ctx, regProfile, password); err != nil {
			return false, err
		}
		c.routeHeaders = registeredInviteRouteHeaders(c.configuredRouteHeaders, regProfile, c.routeRegisteredInviteToRegistrar)
		return true, nil
	case <-ctx.Done():
		return false, ctx.Err()
	case <-c.c.closing.Watch():
		return false, errors.New("SIP client closed")
	}
}

func shouldRetryInviteAfterFreshRegister(resp *sip.Response) bool {
	if resp == nil {
		return false
	}
	switch resp.StatusCode {
	case sip.StatusTemporarilyUnavailable, sip.StatusServiceUnavailable:
		return true
	case sip.StatusForbidden:
		if serviceNotAuthorised(resp) {
			return true
		}
		body := strings.ToLower(string(resp.Body()))
		return strings.Contains(body, "<ims-3gpp") &&
			strings.Contains(body, "<alternative-service>") &&
			strings.Contains(body, "<action>initial-registration</action>")
	default:
		return false
	}
}

func registeredInviteRouteHeaders(configured []string, regProfile *ResolvedRegistrationConfig, routeRegisteredInviteToRegistrar bool) []string {
	if regProfile == nil {
		return cloneRouteHeaders(configured)
	}
	configuredRoutes := cloneRouteHeaders(configured)
	if routeRegisteredInviteToRegistrar {
		configuredRoutes = removeRegistrationRouteHeaders(configuredRoutes, regProfile)
	}
	routes := appendRouteHeaders(configuredRoutes, regProfile.ServiceRouteHeaders)
	if routeRegisteredInviteToRegistrar && len(regProfile.ServiceRouteHeaders) == 0 {
		routes = appendRouteHeaders(routes, []string{registrationRouteHeader(regProfile)})
	}
	return routes
}

func removeRegistrationRouteHeaders(routes []string, conf *ResolvedRegistrationConfig) []string {
	if len(routes) == 0 || conf == nil {
		return routes
	}
	remove := map[string]struct{}{}
	for _, uri := range []URI{conf.Registrar, conf.RouteRegistrar} {
		if host := uri.GetHost(); host != "" {
			remove[strings.ToLower(host)] = struct{}{}
		}
	}
	if len(remove) == 0 {
		return routes
	}

	out := routes[:0]
	for _, route := range routes {
		if registrationRouteMatchesHost(route, remove) {
			continue
		}
		out = append(out, route)
	}
	return out
}

func registrationRouteMatchesHost(route string, hosts map[string]struct{}) bool {
	route = strings.ToLower(strings.TrimSpace(route))
	if route == "" {
		return false
	}
	for host := range hosts {
		if strings.Contains(route, "@"+host) ||
			strings.Contains(route, "sip:"+host) ||
			strings.Contains(route, "sips:"+host) {
			return true
		}
	}
	return false
}

func serviceNotAuthorised(resp *sip.Response) bool {
	if resp == nil {
		return false
	}
	for _, h := range resp.GetHeaders("Warning") {
		if h == nil {
			continue
		}
		if strings.Contains(strings.ToLower(h.Value()), "service not authorised") {
			return true
		}
	}
	return false
}

func (c *sipOutbound) AcceptBye(req *sip.Request, tx sip.ServerTransaction) {
	_ = tx.Respond(sip.NewResponseFromRequest(req, 200, "OK", nil))
	c.mu.Lock()
	defer c.mu.Unlock()
	c.drop() // mark as closed
}

func (c *sipOutbound) AckInviteOK(ctx context.Context) error {
	ctx, span := Tracer.Start(ctx, "sip.outbound.AckInviteOK")
	defer span.End()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.invite == nil || c.inviteOk == nil {
		return psrpc.NewErrorf(psrpc.Canceled, "call already closed")
	}
	return c.c.sipCli.WriteRequest(sip.NewAckRequest(c.invite, c.inviteOk, nil))
}

func (c *sipOutbound) attemptInvite(ctx context.Context, callID sip.CallIDHeader, to *sip.ToHeader, offer []byte, authHeaders map[string]string, headers Headers, setState sipRespFunc, onInviteSent sipInviteSentFunc) (*sip.Request, *sip.Response, error) {
	ctx, span := Tracer.Start(ctx, "sip.outbound.attemptInvite")
	defer span.End()
	req := sip.NewRequest(sip.INVITE, to.Address)
	c.setCSeq(req)
	req.RemoveHeader("Call-ID")
	req.AppendHeader(&callID)

	req.SetBody(offer)
	req.AppendHeader(to)
	req.AppendHeader(c.from)
	req.AppendHeader(c.contact)

	req.AppendHeader(sip.NewHeader("Content-Type", "application/sdp"))
	req.AppendHeader(sip.NewHeader("User-Agent", UserAgent))
	req.AppendHeader(sip.NewHeader("Allow", "INVITE, ACK, CANCEL, BYE, NOTIFY, REFER, MESSAGE, OPTIONS, INFO, SUBSCRIBE"))

	for _, authHeaderName := range []string{"Authorization", "Proxy-Authorization"} {
		if authHeader := authHeaders[authHeaderName]; authHeader != "" {
			req.AppendHeader(sip.NewHeader(authHeaderName, authHeader))
		}
	}
	for _, h := range headers {
		req.AppendHeader(h)
	}

	prependRouteHeaders(req, c.routeHeaders)

	tx, err := c.c.sipCli.TransactionRequest(req)
	if err != nil {
		return nil, nil, err
	}
	defer tx.Terminate()
	if onInviteSent != nil {
		onInviteSent()
	}
	c.log.Infow("SIP INVITE request prepared", inviteRequestLogFields(req)...)

	// Log the actual local port used for TCP connections from the DialPort range
	if req.Transport() == "TCP" {
		// Type-assert to *sipgo.Client to access the embedded UserAgent
		if sipClient, ok := c.c.sipCli.(*sipgo.Client); ok {
			if tpl := sipClient.TransportLayer(); tpl != nil {
				// Try to get the connection using the destination address
				// The connection should be available after TransactionRequest creates it
				if dest := req.Destination(); dest != "" {
					if conn, err := tpl.GetConnection("tcp", dest); err == nil && conn != nil {
						if tcpAddr, ok := conn.LocalAddr().(*net.TCPAddr); ok && tcpAddr != nil {
							c.log.Debugw("TCP connection using port on cloud-sip side", "port", tcpAddr.Port)
						}
					}
				}
			}
		}
	}

	resp, err := sipResponse(ctx, tx, c.c.closing.Watch(), setState)
	return req, resp, err
}

func (c *sipOutbound) logInviteFinalResponse(resp *sip.Response) {
	if resp == nil {
		return
	}
	fields := []interface{}{
		"status", resp.StatusCode,
		"reason", resp.Reason,
		"body", string(resp.Body()),
	}
	for _, name := range []string{"Retry-After", "Reason", "Warning", "Server", "X-Twilio-Error"} {
		if h := resp.GetHeader(name); h != nil {
			fields = append(fields, name, h.Value())
		}
	}
	c.log.Infow("SIP INVITE final response received", fields...)
}

func (c *sipOutbound) logInviteAcceptedResponse(resp *sip.Response) {
	if resp == nil {
		return
	}
	fields := []interface{}{
		"status", resp.StatusCode,
		"reason", resp.Reason,
		"bodyBytes", len(resp.Body()),
	}
	for _, name := range []string{"Contact", "Server", "User-Agent"} {
		if h := resp.GetHeader(name); h != nil {
			fields = append(fields, name, h.Value())
		}
	}
	c.log.Infow("SIP INVITE accepted", fields...)
}

func inviteRequestLogFields(req *sip.Request) []interface{} {
	fields := []interface{}{
		"method", req.Method,
		"request_uri", req.Recipient.String(),
		"transport", req.Transport(),
		"dest_addr", req.Destination(),
		"local_addr", req.Source(),
		"content_length", len(req.Body()),
	}
	if h := req.From(); h != nil {
		fields = append(fields,
			"from_uri", h.Address.String(),
			"from_display_name", h.DisplayName,
		)
	}
	if h := req.To(); h != nil {
		fields = append(fields,
			"to_uri", h.Address.String(),
			"to_display_name", h.DisplayName,
		)
	}
	if h := req.Contact(); h != nil {
		fields = append(fields, "contact_uri", h.Address.String())
	}
	if h := req.Via(); h != nil {
		fields = append(fields,
			"via_sent_by", h.SentBy(),
			"via_transport", h.Transport,
		)
	}
	if h := req.CallID(); h != nil {
		fields = append(fields, "sip_call_id", h.Value())
	}
	if h := req.CSeq(); h != nil {
		fields = append(fields,
			"cseq", h.SeqNo,
			"cseq_method", h.MethodName,
		)
	}
	if h := req.ContentType(); h != nil {
		fields = append(fields, "content_type", h.Value())
	}
	if h := req.GetHeader("User-Agent"); h != nil {
		fields = append(fields, "user_agent", h.Value())
	}
	fields = append(fields,
		"has_authorization", req.GetHeader("Authorization") != nil,
		"has_proxy_authorization", req.GetHeader("Proxy-Authorization") != nil,
		"headers", sanitizedHeaderValues(req.Headers()),
	)
	if routes := headerValues(req.GetHeaders("Route")); len(routes) != 0 {
		fields = append(fields, "route_headers", routes)
	}
	if reasons := headerValues(req.GetHeaders("Reason")); len(reasons) != 0 {
		fields = append(fields, "reason_headers", reasons)
	}
	return fields
}

func inviteAuthChallengeLogFields(status sip.StatusCode, authHeaderName, authHeaderRespName string, challenge *digest.Challenge) []interface{} {
	fields := []interface{}{
		"status", status,
		"auth_challenge_header", authHeaderName,
		"auth_response_header", authHeaderRespName,
	}
	if challenge == nil {
		return fields
	}
	fields = append(fields,
		"realm", challenge.Realm,
		"algorithm", challenge.Algorithm,
		"qop", challenge.QOP,
		"stale", challenge.Stale,
		"domain", challenge.Domain,
		"nonce_hash", shortSHA256Hex(challenge.Nonce),
		"nonce_len", len(challenge.Nonce),
		"opaque_present", challenge.Opaque != "",
	)
	return fields
}

func inviteAuthResponseLogFields(authHeaderRespName string, req *sip.Request, cred *digest.Credentials, digestURI string) []interface{} {
	fields := []interface{}{
		"auth_response_header", authHeaderRespName,
		"digest_uri", digestURI,
		"has_proxy_authorization", authHeaderRespName == "Proxy-Authorization",
	}
	if req != nil {
		fields = append(fields, "request_uri", req.Recipient.String())
	}
	if cred == nil {
		return fields
	}
	fields = append(fields,
		"username", cred.Username,
		"algorithm", cred.Algorithm,
		"qop", cred.QOP,
		"nc", cred.Nc,
	)
	return fields
}

func shortSHA256Hex(value string) string {
	if value == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])[:16]
}

func sanitizedHeaderValues(headers []sip.Header) map[string][]string {
	out := make(map[string][]string, len(headers))
	for _, h := range headers {
		name := h.Name()
		if isSensitiveSIPHeader(name) {
			continue
		}
		out[name] = append(out[name], h.Value())
	}
	return out
}

func isSensitiveSIPHeader(name string) bool {
	name = strings.ToLower(name)
	return name == "authorization" ||
		name == "proxy-authorization" ||
		name == "www-authenticate" ||
		name == "proxy-authenticate"
}

func headerValues(headers []sip.Header) []string {
	if len(headers) == 0 {
		return nil
	}
	values := make([]string, 0, len(headers))
	for _, h := range headers {
		values = append(values, h.Value())
	}
	return values
}

func (c *sipOutbound) WriteRequest(req *sip.Request) error {
	return c.c.sipCli.WriteRequest(req)
}

func (c *sipOutbound) Transaction(req *sip.Request) (sip.ClientTransaction, error) {
	return c.c.sipCli.TransactionRequest(req)
}

func (c *sipOutbound) setCSeq(req *sip.Request) {
	setCSeq(req, c.nextCSeq)

	c.nextCSeq++
}

func (c *sipOutbound) sendBye(ctx context.Context) {
	ctx = context.WithoutCancel(ctx)
	if c.invite == nil || c.inviteOk == nil {
		return // call wasn't established
	}
	ctx, span := Tracer.Start(ctx, "sip.outbound.sendBye")
	defer span.End()
	r := sip.NewByeRequest(c.invite, c.inviteOk, nil)
	r.AppendHeader(sip.NewHeader("User-Agent", "AI-MOP"))
	if c.getHeaders != nil {
		for k, v := range c.getHeaders(nil) {
			r.AppendHeader(sip.NewHeader(k, v))
		}
	}
	if c.c.closing.IsBroken() {
		// do not wait for a response
		_ = c.WriteRequest(r)
		return
	}
	c.setCSeq(r)
	c.drop()
	sendAndACK(ctx, c, r)
}

func (c *sipOutbound) sendCancel(ctx context.Context) {
	ctx = context.WithoutCancel(ctx)
	if c.invite == nil {
		return
	}
	ctx, span := Tracer.Start(ctx, "sip.outbound.sendCancel")
	defer span.End()
	r := sip.NewCancelRequest(c.invite)
	r.AppendHeader(sip.NewHeader("User-Agent", "AI-MOP"))
	if c.getHeaders != nil {
		for k, v := range c.getHeaders(nil) {
			r.AppendHeader(sip.NewHeader(k, v))
		}
	}
	_ = c.WriteRequest(r)
	c.drop()
}

func (c *sipOutbound) drop() {
	c.invite = nil
	c.inviteOk = nil
	c.nextCSeq = 0
}

func (c *sipOutbound) Drop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.drop()
}

func (c *sipOutbound) transferCall(ctx context.Context, transferTo string, headers map[string]string, callDone <-chan struct{}) error {
	c.mu.Lock()

	if c.invite == nil || c.inviteOk == nil {
		c.mu.Unlock()
		return psrpc.NewErrorf(psrpc.FailedPrecondition, "can't transfer non established call") // call wasn't established
	}

	if c.c.closing.IsBroken() {
		c.mu.Unlock()
		return psrpc.NewErrorf(psrpc.FailedPrecondition, "can't transfer hung up call")
	}

	if c.getHeaders != nil {
		headers = c.getHeaders(headers)
	}

	req := NewReferRequest(c.invite, c.inviteOk, c.contact, transferTo, headers)
	c.setCSeq(req)
	cseq := req.CSeq()

	if cseq == nil {
		c.mu.Unlock()
		return psrpc.NewErrorf(psrpc.Internal, "missing CSeq header in REFER request")
	}
	c.referCseq = cseq.SeqNo
	c.mu.Unlock()

	_, err := sendRefer(ctx, c, req, c.c.closing.Watch())
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return psrpc.NewErrorf(psrpc.Canceled, "refer canceled")
	case <-callDone:
		// At this point, REFER was accepted, but we received a BYE, nothing to do, also not an error
		c.log.Infow("refer canceled by BYE from remote")
		return nil
	case err := <-c.referDone:
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *sipOutbound) handleNotify(req *sip.Request, tx sip.ServerTransaction) error {
	method, cseq, status, reason, err := handleNotify(req)
	if err != nil {
		c.log.Infow("error parsing NOTIFY request", "error", err)

		return err
	}

	c.log.Infow("handling NOTIFY", "method", method, "status", status, "reason", reason, "cseq", cseq)

	switch method {
	default:
		return nil
	case sip.REFER:
		c.mu.RLock()
		defer c.mu.RUnlock()
		handleReferNotify(cseq, status, reason, c.referCseq, c.referDone)
		return nil
	}
}

func (c *sipOutbound) Close(ctx context.Context) {
	ctx = context.WithoutCancel(ctx)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.inviteOk != nil {
		c.sendBye(ctx)
	} else if c.invite != nil {
		c.sendCancel(ctx)
	} else {
		c.drop()
	}
}
