package sip

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/icholy/digest"
	"github.com/pkg/errors"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/utils/guid"
	"github.com/livekit/psrpc"
	"github.com/livekit/sipgo/sip"
)

const (
	defaultRegisterExpiration = 300 * time.Second
	defaultRegisterRefresh    = 30 * time.Second
	minRegisterExpiration     = 15 * time.Second
	freshRegisterInviteRetry  = 5 * time.Second
	registrationIdleTimeout   = 10 * time.Minute
	registerRefreshBackoffMin = 5 * time.Second
	registerRefreshBackoffMid = 15 * time.Second
	registerRefreshBackoffMax = 30 * time.Second
	registerAuthMaxAttempts   = 3
	inviteAuthMaxAttempts     = 3
)

var invitePostRegisterSettlingDelay = time.Second

type outboundRegisterMode int32

const (
	outboundRegisterModeAuto outboundRegisterMode = iota
	outboundRegisterModeDisabled
	outboundRegisterModeRequired
)

func (m outboundRegisterMode) String() string {
	switch m {
	case outboundRegisterModeDisabled:
		return "disabled"
	case outboundRegisterModeRequired:
		return "required"
	default:
		return "auto"
	}
}

type ResolvedRegistrationConfig struct {
	RegistrarURI              string
	AuthURI                   string
	AORUser                   string
	AuthUsername              string
	ContactUser               string
	FromDomain                string
	RouteHeaders              []string
	Transport                 Transport
	Expires                   time.Duration
	RefreshBefore             time.Duration
	AlwaysRefreshBeforeInvite bool
	InviteOnRegisterFailure   bool
	Registrar                 URI
}

type registrationState struct {
	expiresAt      time.Time
	lastUsedAt     time.Time
	lastSuccessAt  time.Time
	inflight       chan struct{}
	refreshStarted bool
	err            error
}

type RegistrationManager struct {
	mu     sync.Mutex
	states map[string]*registrationState
}

func NewRegistrationManager() *RegistrationManager {
	return &RegistrationManager{
		states: make(map[string]*registrationState),
	}
}

func (m *RegistrationManager) ensure(ctx context.Context, c *Client, conf *ResolvedRegistrationConfig, password string, contact URI) error {
	if m == nil || conf == nil {
		return nil
	}

	key := conf.cacheKey()
	for {
		m.mu.Lock()
		st := m.states[key]
		if st == nil {
			st = &registrationState{}
			m.states[key] = st
		}
		st.lastUsedAt = time.Now()
		if !conf.AlwaysRefreshBeforeInvite && st.inflight == nil && st.expiresAt.After(time.Now().Add(conf.RefreshBefore)) {
			m.ensureRefreshLoopLocked(c, key, conf, password, contact, st)
			m.mu.Unlock()
			return nil
		}
		if st.inflight != nil {
			wait := st.inflight
			m.mu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-wait:
				continue
			}
		}
		st.inflight = make(chan struct{})
		m.mu.Unlock()

		expires, err := c.register(ctx, conf, password, contact)

		m.mu.Lock()
		if err == nil {
			st.expiresAt = time.Now().Add(expires)
			m.ensureRefreshLoopLocked(c, key, conf, password, contact, st)
		} else {
			st.expiresAt = time.Time{}
		}
		st.err = err
		close(st.inflight)
		st.inflight = nil
		m.mu.Unlock()
		return err
	}
}

func (m *RegistrationManager) ensureRefreshLoopLocked(c *Client, key string, conf *ResolvedRegistrationConfig, password string, contact URI, st *registrationState) {
	if st.refreshStarted || c == nil || c.closing.IsBroken() {
		return
	}
	st.refreshStarted = true
	go m.refreshLoop(c, key, conf.clone(), password, contact)
}

func (m *RegistrationManager) refreshLoop(c *Client, key string, conf *ResolvedRegistrationConfig, password string, contact URI) {
	backoff := registerRefreshBackoffMin
	for {
		wait, stop := m.nextRefreshWait(key, conf)
		if stop {
			return
		}

		select {
		case <-time.After(wait):
		case <-c.closing.Watch():
			return
		}
		if m.stopRefreshIfIdle(key) {
			return
		}

		err := m.refresh(context.WithoutCancel(context.Background()), c, key, conf, password, contact)
		if err == nil {
			backoff = registerRefreshBackoffMin
			continue
		}
		c.log.Warnw("SIP REGISTER background refresh failed", err,
			"registrar", conf.Registrar.GetDest(),
			"backoff", backoff,
		)
		select {
		case <-time.After(backoff):
		case <-c.closing.Watch():
			return
		}
		switch backoff {
		case registerRefreshBackoffMin:
			backoff = registerRefreshBackoffMid
		default:
			backoff = registerRefreshBackoffMax
		}
	}
}

func (m *RegistrationManager) stopRefreshIfIdle(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	st := m.states[key]
	if st == nil {
		return true
	}
	if time.Since(st.lastUsedAt) <= registrationIdleTimeout {
		return false
	}
	st.refreshStarted = false
	return true
}

func (m *RegistrationManager) nextRefreshWait(key string, conf *ResolvedRegistrationConfig) (time.Duration, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st := m.states[key]
	if st == nil {
		return 0, true
	}
	if time.Since(st.lastUsedAt) > registrationIdleTimeout {
		st.refreshStarted = false
		return 0, true
	}
	if !st.expiresAt.IsZero() && !st.expiresAt.After(time.Now()) {
		st.expiresAt = time.Time{}
	}
	if st.expiresAt.IsZero() {
		return registerRefreshBackoffMin, false
	}
	return time.Until(st.expiresAt.Add(-registrationRefreshLead(st.expiresAt, conf.RefreshBefore))), false
}

func (m *RegistrationManager) refresh(ctx context.Context, c *Client, key string, conf *ResolvedRegistrationConfig, password string, contact URI) error {
	for {
		m.mu.Lock()
		st := m.states[key]
		if st == nil {
			st = &registrationState{}
			m.states[key] = st
		}
		if st.inflight != nil {
			wait := st.inflight
			m.mu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-wait:
				return nil
			}
		}
		st.inflight = make(chan struct{})
		m.mu.Unlock()

		expires, err := c.register(ctx, conf, password, contact)

		m.mu.Lock()
		if err == nil {
			st.expiresAt = time.Now().Add(expires)
		} else if !st.expiresAt.After(time.Now()) {
			st.expiresAt = time.Time{}
		}
		st.err = err
		close(st.inflight)
		st.inflight = nil
		m.mu.Unlock()
		return err
	}
}

func registrationRefreshLead(expiresAt time.Time, refreshBefore time.Duration) time.Duration {
	ttl := time.Until(expiresAt)
	if ttl <= 0 {
		return 0
	}
	if refreshBefore > 0 && ttl > 2*refreshBefore {
		return refreshBefore
	}
	return ttl / 2
}

func (m *RegistrationManager) markSuccessfulRegister(key string, at time.Time) {
	if m == nil || key == "" || at.IsZero() {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	st := m.states[key]
	if st == nil {
		st = &registrationState{}
		m.states[key] = st
	}
	st.lastSuccessAt = at
}

func (m *RegistrationManager) freshSuccessfulRegisterAge(key string, maxAge time.Duration) (time.Duration, bool) {
	if m == nil {
		return 0, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	st := m.states[key]
	if st == nil || st.lastSuccessAt.IsZero() {
		return 0, false
	}
	age := time.Since(st.lastSuccessAt)
	return age, age >= 0 && age <= maxAge
}

func (m *RegistrationManager) waitForRegisterSettling(ctx context.Context, key string, minAge time.Duration) error {
	if m == nil || key == "" || minAge <= 0 {
		return nil
	}
	age, ok := m.freshSuccessfulRegisterAge(key, minAge)
	if !ok {
		return nil
	}
	wait := minAge - age
	if wait <= 0 {
		return nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (c *ResolvedRegistrationConfig) clone() *ResolvedRegistrationConfig {
	if c == nil {
		return nil
	}
	cp := *c
	return &cp
}

func (c *ResolvedRegistrationConfig) cacheKey() string {
	if c == nil {
		return ""
	}
	return strings.Join([]string{
		c.Registrar.GetDest(),
		string(c.Transport),
		c.AuthUsername,
		c.AORUser,
		c.ContactUser,
		c.FromDomain,
		c.AuthURI,
		strings.Join(c.RouteHeaders, ","),
	}, "|")
}

func (c *Client) ensureRegistered(ctx context.Context, sipConf sipOutboundConfig) (*ResolvedRegistrationConfig, error) {
	if c == nil || c.sipCli == nil || c.sconf == nil {
		return nil, nil
	}
	if sipConf.user == "" || sipConf.pass == "" || sipConf.address == "" {
		return nil, nil
	}

	conf, err := resolveRegistrationConfig(sipConf)
	if err != nil || conf == nil {
		return conf, err
	}

	contact := c.ContactURI(conf.Transport)
	contact.User = conf.ContactUser
	return conf, c.registrationManager.ensure(ctx, c, conf, sipConf.pass, contact)
}

func (c *Client) forceRegister(ctx context.Context, conf *ResolvedRegistrationConfig, password string) error {
	if c == nil || c.sipCli == nil || c.sconf == nil || conf == nil {
		return nil
	}
	forceConf := conf.clone()
	forceConf.AlwaysRefreshBeforeInvite = true

	contact := c.ContactURI(forceConf.Transport)
	contact.User = forceConf.ContactUser
	return c.registrationManager.ensure(ctx, c, forceConf, password, contact)
}

func resolveRegistrationConfig(sipConf sipOutboundConfig) (*ResolvedRegistrationConfig, error) {
	conf := &ResolvedRegistrationConfig{
		InviteOnRegisterFailure:   true,
		Transport:                 TransportFrom(sipConf.transport),
		Expires:                   defaultRegisterExpiration,
		RefreshBefore:             defaultRegisterRefresh,
		RegistrarURI:              "sip:" + sipConf.address,
		AORUser:                   sipConf.user,
		AuthUsername:              sipConf.user,
		ContactUser:               sipConf.user,
		FromDomain:                sipConf.host,
		RouteHeaders:              appendRouteHeaders(sipConf.routeHeaders, routeHeadersFromHeaderMap(sipConf.headers)),
		AlwaysRefreshBeforeInvite: false,
	}
	if conf.Transport == "" {
		conf.Transport = TransportUDP
	}

	var err error
	conf.RegistrarURI = strings.TrimSpace(conf.RegistrarURI)
	if conf.RegistrarURI == "" {
		return nil, psrpc.NewError(psrpc.InvalidArgument, errors.New("sip registration requires trunk address"))
	}
	conf.Registrar, err = parseRegistrationURI(conf.RegistrarURI, conf.Transport)
	if err != nil {
		return nil, err
	}
	if conf.AORUser == "" {
		return nil, psrpc.NewError(psrpc.InvalidArgument, errors.New("sip registration requires auth username"))
	}
	if conf.FromDomain == "" {
		conf.FromDomain = conf.Registrar.GetHost()
	}
	conf.AuthURI = normalizeRegisterAuthURI(conf.Registrar, false, false)
	return conf, nil
}

func (c *Client) register(ctx context.Context, conf *ResolvedRegistrationConfig, password string, contact URI) (time.Duration, error) {
	callID := sip.CallIDHeader(guid.New("reg_"))
	fromTag := sip.GenerateTagN(16)
	authHeaders := make(map[string]string)
	cacheKey := conf.cacheKey()

	for attempt := 0; attempt < registerAuthMaxAttempts; attempt++ {
		req := c.newRegisterRequest(conf, contact, fromTag, uint32(attempt+1), callID, authHeaders)
		c.log.Infow("sending SIP REGISTER",
			"attempt", attempt+1,
			"registrar", conf.Registrar.GetDest(),
			"aorUser", conf.AORUser,
			"fromDomain", conf.FromDomain,
			"authUser", conf.AuthUsername,
			"authURI", conf.AuthURI,
			"transport", conf.Transport,
			"expiresSec", int(conf.Expires/time.Second),
			"hasAuthorization", authHeaders["Authorization"] != "",
			"hasProxyAuthorization", authHeaders["Proxy-Authorization"] != "",
		)
		tx, err := c.sipCli.TransactionRequest(req)
		if err != nil {
			c.log.Warnw("SIP REGISTER transaction failed", err,
				"attempt", attempt+1,
				"registrar", conf.Registrar.GetDest(),
			)
			return 0, err
		}

		resp, err := sipResponse(ctx, tx, c.closing.Watch(), nil)
		tx.Terminate()
		if err != nil {
			c.log.Warnw("SIP REGISTER failed waiting for response", err,
				"attempt", attempt+1,
				"registrar", conf.Registrar.GetDest(),
			)
			return 0, err
		}
		c.log.Infow("SIP REGISTER response received",
			"attempt", attempt+1,
			"registrar", conf.Registrar.GetDest(),
			"status", resp.StatusCode,
			"reason", resp.Reason,
		)

		authHeaderName := ""
		authHeaderRespName := ""
		switch resp.StatusCode {
		case sip.StatusOK:
			expires := registrationExpires(resp, conf.Expires)
			c.registrationManager.markSuccessfulRegister(cacheKey, time.Now())
			c.log.Infow("SIP REGISTER succeeded",
				"registrar", conf.Registrar.GetDest(),
				"expiresSec", int(expires/time.Second),
			)
			return expires, nil
		case sip.StatusUnauthorized:
			authHeaderName = "WWW-Authenticate"
			authHeaderRespName = "Authorization"
		case sip.StatusProxyAuthRequired:
			authHeaderName = "Proxy-Authenticate"
			authHeaderRespName = "Proxy-Authorization"
		case sip.StatusForbidden, sip.StatusNotFound, sip.StatusMethodNotAllowed, sip.StatusNotImplemented:
			c.log.Warnw("SIP REGISTER rejected", nil,
				"registrar", conf.Registrar.GetDest(),
				"status", resp.StatusCode,
				"reason", resp.Reason,
			)
			return 0, fmt.Errorf("REGISTER failed: %w", &livekit.SIPStatus{
				Code:   livekit.SIPStatusCode(resp.StatusCode),
				Status: resp.Reason,
			})
		default:
			c.log.Warnw("SIP REGISTER failed", nil,
				"registrar", conf.Registrar.GetDest(),
				"status", resp.StatusCode,
				"reason", resp.Reason,
			)
			return 0, fmt.Errorf("REGISTER failed: %w", &livekit.SIPStatus{
				Code:   livekit.SIPStatusCode(resp.StatusCode),
				Status: resp.Reason,
			})
		}

		challengeHeader := resp.GetHeader(authHeaderName)
		if challengeHeader == nil {
			return 0, psrpc.NewError(psrpc.FailedPrecondition, errors.New("no auth header in sip register response"))
		}
		c.log.Infow("SIP REGISTER auth challenge received",
			"attempt", attempt+1,
			"registrar", conf.Registrar.GetDest(),
			"authChallengeHeader", authHeaderName,
			"authResponseHeader", authHeaderRespName,
		)
		challenge, err := digest.ParseChallenge(challengeHeader.Value())
		if err != nil {
			return 0, fmt.Errorf("invalid register challenge %q: %w", challengeHeader.Value(), err)
		}
		digestURI := req.Recipient.String()
		if digestURI == "" {
			digestURI = conf.AuthURI
		}
		cred, err := digest.Digest(challenge, digest.Options{
			Method:   sip.REGISTER.String(),
			URI:      digestURI,
			Username: conf.AuthUsername,
			Password: password,
		})
		if err != nil {
			return 0, err
		}
		authHeaders[authHeaderRespName] = cred.String()
	}

	c.log.Warnw("SIP REGISTER exhausted retry attempts", nil,
		"registrar", conf.Registrar.GetDest(),
	)
	return 0, psrpc.NewError(psrpc.FailedPrecondition, fmt.Errorf("max auth retry attempts reached for SIP register"))
}

func registrationExpires(resp *sip.Response, fallback time.Duration) time.Duration {
	normalize := func(v time.Duration) time.Duration {
		if v < minRegisterExpiration {
			return minRegisterExpiration
		}
		return v
	}
	if resp == nil {
		return normalize(fallback)
	}
	if contact := resp.Contact(); contact != nil {
		if raw, ok := headerParam(contact.Params, "expires"); ok {
			if seconds, err := strconv.Atoi(raw); err == nil && seconds >= 0 {
				return normalize(time.Duration(seconds) * time.Second)
			}
		}
	}
	if header := resp.GetHeader("Expires"); header != nil {
		if seconds, err := strconv.Atoi(strings.TrimSpace(header.Value())); err == nil && seconds >= 0 {
			return normalize(time.Duration(seconds) * time.Second)
		}
	}
	return normalize(fallback)
}

func headerParam(params sip.HeaderParams, name string) (string, bool) {
	for _, param := range params {
		if strings.EqualFold(param.K, name) {
			return param.V, true
		}
	}
	return "", false
}

func (c *Client) newRegisterRequest(conf *ResolvedRegistrationConfig, contact URI, fromTag string, cseq uint32, callID sip.CallIDHeader, authHeaders map[string]string) *sip.Request {
	registerURI := conf.Registrar.GetURI()
	aorURI := &sip.Uri{
		Scheme: "sip",
		User:   conf.AORUser,
		Host:   conf.FromDomain,
	}
	contactURI := *contact.GetContactURI()
	maxForwards := sip.MaxForwardsHeader(70)

	req := sip.NewRequest(sip.REGISTER, *registerURI)
	req.SetDestination(conf.Registrar.GetDest())
	req.AppendHeader(&sip.ToHeader{Address: *aorURI})
	req.AppendHeader(&sip.FromHeader{
		Address: *aorURI,
		Params:  sip.HeaderParams{{K: "tag", V: fromTag}},
	})
	req.AppendHeader(&sip.ContactHeader{Address: contactURI})
	req.AppendHeader(&callID)
	req.AppendHeader(&sip.CSeqHeader{SeqNo: cseq, MethodName: sip.REGISTER})
	req.AppendHeader(&maxForwards)
	prependRouteHeaders(req, conf.RouteHeaders)
	req.AppendHeader(sip.NewHeader("Expires", strconv.Itoa(int(conf.Expires/time.Second))))
	req.AppendHeader(sip.NewHeader("User-Agent", UserAgent))
	req.AppendHeader(sip.NewHeader("Allow", "INVITE, ACK, CANCEL, BYE, NOTIFY, REFER, MESSAGE, OPTIONS, INFO, SUBSCRIBE, REGISTER"))
	for _, authHeaderName := range []string{"Authorization", "Proxy-Authorization"} {
		if authHeaderValue := authHeaders[authHeaderName]; authHeaderValue != "" {
			req.AppendHeader(sip.NewHeader(authHeaderName, authHeaderValue))
		}
	}
	return req
}

func routeHeadersFromHeaderMap(headers map[string]string) []string {
	for name, value := range headers {
		if !strings.EqualFold(name, "Route") {
			continue
		}
		value = strings.TrimSpace(value)
		if value == "" {
			return nil
		}
		return []string{value}
	}
	return nil
}

func parseRegistrationURI(raw string, fallbackTransport Transport) (URI, error) {
	raw = strings.TrimSpace(raw)
	raw = strings.TrimPrefix(raw, "sip:")
	raw = strings.TrimPrefix(raw, "sips:")
	transport := fallbackTransport
	if i := strings.Index(raw, ";"); i >= 0 {
		params := raw[i+1:]
		raw = raw[:i]
		for _, p := range strings.Split(params, ";") {
			if key, val, ok := strings.Cut(p, "="); ok && strings.EqualFold(key, "transport") {
				transport = Transport(strings.ToLower(val))
			}
		}
	}
	return CreateURIFromUserAndAddress("", raw, transport), nil
}

func normalizeRegisterAuthURI(registrar URI, includePort bool, includeTransport bool) string {
	return buildSIPURI(registrar.GetHost(), registrar.Transport, includePort, includeTransport)
}

func buildSIPURI(host string, transport Transport, includePort bool, includeTransport bool) string {
	if host == "" {
		return ""
	}
	uri := "sip:" + host
	if includePort {
		port := 5060
		if transport == TransportTLS {
			port = 5061
		}
		uri += ":" + strconv.Itoa(port)
	}
	if includeTransport && transport != "" {
		uri += ";transport=" + string(transport)
	}
	return uri
}

func stringsForAuthHeader(authHeaderName string) string {
	switch authHeaderName {
	case "Authorization":
		return "WWW-Authenticate"
	case "Proxy-Authorization":
		return "Proxy-Authenticate"
	default:
		return ""
	}
}
