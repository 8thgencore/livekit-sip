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
)

type ResolvedRegistrationConfig struct {
	RegistrarURI              string
	AuthURI                   string
	AORUser                   string
	AuthUsername              string
	ContactUser               string
	FromDomain                string
	Transport                 Transport
	Expires                   time.Duration
	RefreshBefore             time.Duration
	AlwaysRefreshBeforeInvite bool
	InviteOnRegisterFailure   bool
	Registrar                 URI
}

type registrationState struct {
	expiresAt time.Time
	inflight  chan struct{}
	err       error
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
		if !conf.AlwaysRefreshBeforeInvite && st.inflight == nil && st.expiresAt.After(time.Now().Add(conf.RefreshBefore)) {
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

		err := c.register(ctx, conf, password, contact)

		m.mu.Lock()
		if err == nil {
			st.expiresAt = time.Now().Add(conf.Expires)
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

func (c *Client) register(ctx context.Context, conf *ResolvedRegistrationConfig, password string, contact URI) error {
	callID := sip.CallIDHeader(guid.New("reg_"))
	fromTag := sip.GenerateTagN(16)
	authHeaderName := ""
	authHeaderValue := ""

	for attempt := 0; attempt < 5; attempt++ {
		req := c.newRegisterRequest(conf, contact, fromTag, uint32(attempt+1), callID, authHeaderName, authHeaderValue)
		c.log.Infow("sending SIP REGISTER",
			"attempt", attempt+1,
			"registrar", conf.Registrar.GetDest(),
			"aorUser", conf.AORUser,
			"fromDomain", conf.FromDomain,
			"authUser", conf.AuthUsername,
			"authURI", conf.AuthURI,
			"transport", conf.Transport,
			"expiresSec", int(conf.Expires/time.Second),
			"hasAuthorization", authHeaderValue != "",
		)
		tx, err := c.sipCli.TransactionRequest(req)
		if err != nil {
			c.log.Warnw("SIP REGISTER transaction failed", err,
				"attempt", attempt+1,
				"registrar", conf.Registrar.GetDest(),
			)
			return err
		}

		resp, err := sipResponse(ctx, tx, c.closing.Watch(), nil)
		tx.Terminate()
		if err != nil {
			c.log.Warnw("SIP REGISTER failed waiting for response", err,
				"attempt", attempt+1,
				"registrar", conf.Registrar.GetDest(),
			)
			return err
		}
		c.log.Infow("SIP REGISTER response received",
			"attempt", attempt+1,
			"registrar", conf.Registrar.GetDest(),
			"status", resp.StatusCode,
			"reason", resp.Reason,
		)

		switch resp.StatusCode {
		case sip.StatusOK:
			c.log.Infow("SIP REGISTER succeeded",
				"registrar", conf.Registrar.GetDest(),
				"expiresSec", int(conf.Expires/time.Second),
			)
			return nil
		case sip.StatusUnauthorized:
			authHeaderName = "Authorization"
		case sip.StatusProxyAuthRequired:
			authHeaderName = "Proxy-Authorization"
		case sip.StatusForbidden, sip.StatusNotFound, sip.StatusMethodNotAllowed, sip.StatusNotImplemented:
			c.log.Warnw("SIP REGISTER rejected", nil,
				"registrar", conf.Registrar.GetDest(),
				"status", resp.StatusCode,
				"reason", resp.Reason,
			)
			return fmt.Errorf("REGISTER failed: %w", &livekit.SIPStatus{
				Code:   livekit.SIPStatusCode(resp.StatusCode),
				Status: resp.Reason,
			})
		default:
			c.log.Warnw("SIP REGISTER failed", nil,
				"registrar", conf.Registrar.GetDest(),
				"status", resp.StatusCode,
				"reason", resp.Reason,
			)
			return fmt.Errorf("REGISTER failed: %w", &livekit.SIPStatus{
				Code:   livekit.SIPStatusCode(resp.StatusCode),
				Status: resp.Reason,
			})
		}

		challengeHeader := resp.GetHeader(stringsForAuthHeader(authHeaderName))
		if challengeHeader == nil {
			return psrpc.NewError(psrpc.FailedPrecondition, errors.New("no auth header in sip register response"))
		}
		c.log.Infow("SIP REGISTER auth challenge received",
			"attempt", attempt+1,
			"registrar", conf.Registrar.GetDest(),
			"authHeader", authHeaderName,
		)
		challenge, err := digest.ParseChallenge(challengeHeader.Value())
		if err != nil {
			return fmt.Errorf("invalid register challenge %q: %w", challengeHeader.Value(), err)
		}
		cred, err := digest.Digest(challenge, digest.Options{
			Method:   sip.REGISTER.String(),
			URI:      conf.AuthURI,
			Username: conf.AuthUsername,
			Password: password,
		})
		if err != nil {
			return err
		}
		authHeaderValue = cred.String()
	}

	c.log.Warnw("SIP REGISTER exhausted retry attempts", nil,
		"registrar", conf.Registrar.GetDest(),
	)
	return psrpc.NewError(psrpc.FailedPrecondition, fmt.Errorf("max auth retry attempts reached for SIP register"))
}

func (c *Client) newRegisterRequest(conf *ResolvedRegistrationConfig, contact URI, fromTag string, cseq uint32, callID sip.CallIDHeader, authHeaderName, authHeaderValue string) *sip.Request {
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
	req.AppendHeader(sip.NewHeader("Expires", strconv.Itoa(int(conf.Expires/time.Second))))
	req.AppendHeader(sip.NewHeader("User-Agent", UserAgent))
	req.AppendHeader(sip.NewHeader("Allow", "INVITE, ACK, CANCEL, BYE, NOTIFY, REFER, MESSAGE, OPTIONS, INFO, SUBSCRIBE, REGISTER"))
	if authHeaderName != "" && authHeaderValue != "" {
		req.AppendHeader(sip.NewHeader(authHeaderName, authHeaderValue))
	}
	return req
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
