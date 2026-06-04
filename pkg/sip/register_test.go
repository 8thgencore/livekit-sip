package sip

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/icholy/digest"
	"github.com/stretchr/testify/require"

	emisip "github.com/emiago/sipgo/sip"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/sipgo/sip"
)

const (
	mockRegistrarHost = "registrar.example.com"
	mockAuthUser      = "test-auth-user"
	mockAuthPassword  = "test-password"
)

func TestEnsureRegisteredHandlesDigestChallenge(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := client.ensureRegistered(ctx, sipOutboundConfig{
			address: mockRegistrarHost + ":5060",
			host:    mockRegistrarHost,
			user:    mockAuthUser,
			pass:    mockAuthPassword,
		})
		done <- err
	}()

	firstTx := waitTransaction(t, sipClient)
	require.Equal(t, sip.REGISTER, firstTx.req.Method)
	require.Nil(t, firstTx.req.GetHeader("Authorization"))

	challenge := digest.Challenge{
		Realm: mockRegistrarHost,
		Nonce: "nonce-1",
	}
	unauthorized := sip.NewResponseFromRequest(firstTx.req, sip.StatusUnauthorized, "Unauthorized", nil)
	unauthorized.AppendHeader(sip.NewHeader("WWW-Authenticate", challenge.String()))
	require.NoError(t, firstTx.transaction.SendResponse(unauthorized))

	secondTx := waitTransaction(t, sipClient)
	require.Equal(t, sip.REGISTER, secondTx.req.Method)
	authHeader := secondTx.req.GetHeader("Authorization")
	require.NotNil(t, authHeader)
	require.Contains(t, authHeader.Value(), `username="test-auth-user"`)

	ok := sip.NewResponseFromRequest(secondTx.req, sip.StatusOK, "OK", nil)
	require.NoError(t, secondTx.transaction.SendResponse(ok))
	require.NoError(t, <-done)
}

func TestEnsureRegisteredDigestURIUsesRequestURIWithPort(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)

	done := make(chan error, 1)
	go func() {
		_, err := client.ensureRegistered(context.Background(), sipOutboundConfig{
			address: "voip.uiscom.ru:9060",
			host:    "voip.uiscom.ru",
			user:    mockAuthUser,
			pass:    mockAuthPassword,
		})
		done <- err
	}()

	firstTx := waitTransaction(t, sipClient)
	require.Equal(t, sip.REGISTER, firstTx.req.Method)
	require.Equal(t, "sip:voip.uiscom.ru:9060;transport=udp", firstTx.req.Recipient.String())

	challenge := digest.Challenge{
		Realm:     "voip.uiscom.ru",
		Nonce:     "nonce-uiscom",
		Algorithm: "MD5",
		QOP:       []string{"auth"},
	}
	unauthorized := sip.NewResponseFromRequest(firstTx.req, sip.StatusUnauthorized, "Unauthorized", nil)
	unauthorized.AppendHeader(sip.NewHeader("WWW-Authenticate", challenge.String()))
	require.NoError(t, firstTx.transaction.SendResponse(unauthorized))

	secondTx := waitTransaction(t, sipClient)
	require.Equal(t, sip.REGISTER, secondTx.req.Method)
	authHeader := secondTx.req.GetHeader("Authorization")
	require.NotNil(t, authHeader)
	cred, err := digest.ParseCredentials(authHeader.Value())
	require.NoError(t, err)
	require.Equal(t, firstTx.req.Recipient.String(), cred.URI)

	require.NoError(t, secondTx.transaction.SendResponse(sip.NewResponseFromRequest(secondTx.req, sip.StatusOK, "OK", nil)))
	require.NoError(t, <-done)
}

func TestEnsureRegisteredBeelineRefreshesRegistrationEveryTime(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)

	sipConf := sipOutboundConfig{
		address: "ip.beeline.ru:5060",
		host:    "ip.beeline.ru",
		user:    mockAuthUser,
		pass:    mockAuthPassword,
	}

	firstDone := make(chan error, 1)
	go func() {
		_, err := client.ensureRegistered(context.Background(), sipConf)
		firstDone <- err
	}()
	firstTx := waitTransaction(t, sipClient)
	require.Equal(t, sip.REGISTER, firstTx.req.Method)
	firstRoutes := firstTx.req.GetHeaders("Route")
	require.Len(t, firstRoutes, 1)
	require.Equal(t, "<sip:212.119.246.230:5060;transport=udp;lr>", firstRoutes[0].Value())
	require.Equal(t, "212.119.246.230:5060", firstTx.req.Destination())
	ok := sip.NewResponseFromRequest(firstTx.req, sip.StatusOK, "OK", nil)
	ok.AppendHeader(sip.NewHeader("Expires", "150"))
	ok.AppendHeader(sip.NewHeader("Service-Route", "<sip:212.119.246.230:5060;transport=udp;lr;mpcftk=1-115-30c-8-4006a2a2>"))
	require.NoError(t, firstTx.transaction.SendResponse(ok))
	require.NoError(t, <-firstDone)

	secondDone := make(chan error, 1)
	go func() {
		_, err := client.ensureRegistered(context.Background(), sipConf)
		secondDone <- err
	}()
	secondTx := waitTransaction(t, sipClient)
	require.Equal(t, sip.REGISTER, secondTx.req.Method)
	require.Equal(t, "212.119.246.230:5060", secondTx.req.Destination())
	ok = sip.NewResponseFromRequest(secondTx.req, sip.StatusOK, "OK", nil)
	ok.AppendHeader(sip.NewHeader("Expires", "150"))
	ok.AppendHeader(sip.NewHeader("Service-Route", "<sip:212.119.246.230:5060;transport=udp;lr;mpcftk=1-115-30c-8-4006a2a2>"))
	require.NoError(t, secondTx.transaction.SendResponse(ok))
	require.NoError(t, <-secondDone)
}

func TestEnsureRegisteredBeelineRefreshesRegistrationOlderThanInviteMaxAge(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)

	sipConf := sipOutboundConfig{
		address: "ip.beeline.ru:5060",
		host:    "ip.beeline.ru",
		user:    mockAuthUser,
		pass:    mockAuthPassword,
	}

	firstDone := make(chan error, 1)
	go func() {
		_, err := client.ensureRegistered(context.Background(), sipConf)
		firstDone <- err
	}()
	firstTx := waitTransaction(t, sipClient)
	require.Equal(t, sip.REGISTER, firstTx.req.Method)
	ok := sip.NewResponseFromRequest(firstTx.req, sip.StatusOK, "OK", nil)
	ok.AppendHeader(sip.NewHeader("Expires", "150"))
	ok.AppendHeader(sip.NewHeader("Service-Route", "<sip:212.119.246.230:5060;transport=udp;lr;mpcftk=1-115-30c-8-4006a2a2>"))
	require.NoError(t, firstTx.transaction.SendResponse(ok))
	require.NoError(t, <-firstDone)

	type ensureResult struct {
		conf *ResolvedRegistrationConfig
		err  error
	}
	secondDone := make(chan ensureResult, 1)
	go func() {
		regConf, err := client.ensureRegistered(context.Background(), sipConf)
		secondDone <- ensureResult{conf: regConf, err: err}
	}()
	secondTx := waitTransaction(t, sipClient)
	require.Equal(t, sip.REGISTER, secondTx.req.Method)
	ok = sip.NewResponseFromRequest(secondTx.req, sip.StatusOK, "OK", nil)
	ok.AppendHeader(sip.NewHeader("Expires", "150"))
	ok.AppendHeader(sip.NewHeader("Service-Route", "<sip:212.119.246.230:5060;transport=udp;lr;mpcftk=1-115-30c-8-4006a2a2>"))
	require.NoError(t, secondTx.transaction.SendResponse(ok))
	res := <-secondDone
	require.NoError(t, res.err)
	require.Equal(t, []string{
		"<sip:212.119.246.230:5060;transport=udp;lr;mpcftk=1-115-30c-8-4006a2a2>",
	}, res.conf.ServiceRouteHeaders)
}

func TestEnsureRegisteredBeelineRefreshesCachedRegistrationMissingServiceRoute(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)

	sipConf := sipOutboundConfig{
		address: "ip.beeline.ru:5060",
		host:    "ip.beeline.ru",
		user:    mockAuthUser,
		pass:    mockAuthPassword,
	}
	conf, err := resolveRegistrationConfig(sipConf)
	require.NoError(t, err)
	profile := outboundProviderProfileForAddress(sipConf.address)
	conf.MaxAgeBeforeInvite = profile.MaxRegistrationAgeBeforeInvite
	if profile.RouteRegistrationToRegistrar {
		conf.RouteHeaders = appendRouteHeaders(conf.RouteHeaders, []string{registrationRouteHeader(conf)})
	}

	client.registrationManager.mu.Lock()
	client.registrationManager.states[conf.cacheKey()] = &registrationState{
		expiresAt:        time.Now().Add(2 * time.Minute),
		lastSuccessAt:    time.Now(),
		identityKey:      conf.identityCacheKey(),
		looseIdentityKey: conf.looseIdentityCacheKey(),
	}
	client.registrationManager.mu.Unlock()

	type ensureResult struct {
		conf *ResolvedRegistrationConfig
		err  error
	}
	done := make(chan ensureResult, 1)
	go func() {
		regConf, err := client.ensureRegistered(context.Background(), sipConf)
		done <- ensureResult{conf: regConf, err: err}
	}()

	tx := waitTransaction(t, sipClient)
	require.Equal(t, sip.REGISTER, tx.req.Method)
	ok := sip.NewResponseFromRequest(tx.req, sip.StatusOK, "OK", nil)
	ok.AppendHeader(sip.NewHeader("Expires", "150"))
	ok.AppendHeader(sip.NewHeader("Service-Route", "<sip:212.119.246.230:5060;transport=udp;lr;mpcftk=1-115-30c-8-4006a2a2>"))
	require.NoError(t, tx.transaction.SendResponse(ok))

	res := <-done
	require.NoError(t, res.err)
	require.Equal(t, []string{
		"<sip:212.119.246.230:5060;transport=udp;lr;mpcftk=1-115-30c-8-4006a2a2>",
	}, res.conf.ServiceRouteHeaders)
}

func TestEnsureRegisteredBeelineAllowsFreshRegistrationWithoutServiceRoute(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)

	type ensureResult struct {
		conf *ResolvedRegistrationConfig
		err  error
	}
	done := make(chan ensureResult, 1)
	go func() {
		conf, err := client.ensureRegistered(context.Background(), sipOutboundConfig{
			address: "ip.beeline.ru:5060",
			host:    "ip.beeline.ru",
			user:    mockAuthUser,
			pass:    mockAuthPassword,
		})
		done <- ensureResult{conf: conf, err: err}
	}()

	tx := waitTransaction(t, sipClient)
	require.Equal(t, sip.REGISTER, tx.req.Method)
	ok := sip.NewResponseFromRequest(tx.req, sip.StatusOK, "OK", nil)
	ok.AppendHeader(sip.NewHeader("Expires", "150"))
	require.NoError(t, tx.transaction.SendResponse(ok))

	res := <-done
	require.NoError(t, res.err)
	require.NotNil(t, res.conf)
	require.Empty(t, res.conf.ServiceRouteHeaders)
}

func TestEnsureRegisteredBeelineDoesNotUseCachedServiceRouteWithoutPassword(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)

	registeredConf := sipOutboundConfig{
		address: "ip.beeline.ru:5060",
		host:    "ip.beeline.ru",
		user:    mockAuthUser,
		pass:    mockAuthPassword,
	}

	done := make(chan error, 1)
	go func() {
		_, err := client.ensureRegistered(context.Background(), registeredConf)
		done <- err
	}()
	tx := waitTransaction(t, sipClient)
	require.Equal(t, sip.REGISTER, tx.req.Method)
	ok := sip.NewResponseFromRequest(tx.req, sip.StatusOK, "OK", nil)
	ok.AppendHeader(sip.NewHeader("Expires", "150"))
	ok.AppendHeader(sip.NewHeader("Service-Route", "<sip:212.119.246.230:5060;transport=udp;lr;mpcftk=1-115-30c-8-4006a2a2>"))
	require.NoError(t, tx.transaction.SendResponse(ok))
	require.NoError(t, <-done)

	cachedConf, err := client.ensureRegistered(context.Background(), sipOutboundConfig{
		address: "ip.beeline.ru:5060",
		host:    "81.29.140.248",
		from:    mockAuthUser,
		routeHeaders: []string{
			"<sip:212.119.246.230:5060;transport=udp;lr>",
		},
	})
	require.NoError(t, err)
	require.Nil(t, cachedConf)

	select {
	case tx = <-sipClient.transactions:
		t.Fatalf("unexpected REGISTER transaction for cached route lookup: %v", tx.req.Method)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestEnsureRegisteredKeepsProxyAuthorizationWhenWWWAuthenticateFollows(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)

	done := make(chan error, 1)
	go func() {
		_, err := client.ensureRegistered(context.Background(), sipOutboundConfig{
			address: "registrar.proxy-auth.example:5060",
			host:    "proxy-auth.example",
			user:    mockAuthUser,
			pass:    mockAuthPassword,
		})
		done <- err
	}()

	firstTx := waitTransaction(t, sipClient)
	require.Equal(t, sip.REGISTER, firstTx.req.Method)
	proxyChallenge := digest.Challenge{
		Realm: "proxy-auth.example",
		Nonce: "nonce-proxy",
	}
	proxyRequired := sip.NewResponseFromRequest(firstTx.req, sip.StatusProxyAuthRequired, "Proxy Authentication Required", nil)
	proxyRequired.AppendHeader(sip.NewHeader("Proxy-Authenticate", proxyChallenge.String()))
	require.NoError(t, firstTx.transaction.SendResponse(proxyRequired))

	proxyAuthTx := waitTransaction(t, sipClient)
	require.Equal(t, sip.REGISTER, proxyAuthTx.req.Method)
	require.NotNil(t, proxyAuthTx.req.GetHeader("Proxy-Authorization"))
	require.Nil(t, proxyAuthTx.req.GetHeader("Authorization"))
	wwwChallenge := digest.Challenge{
		Realm: "proxy-auth.example",
		Nonce: "nonce-www",
	}
	unauthorized := sip.NewResponseFromRequest(proxyAuthTx.req, sip.StatusUnauthorized, "Unauthorized", nil)
	unauthorized.AppendHeader(sip.NewHeader("WWW-Authenticate", wwwChallenge.String()))
	require.NoError(t, proxyAuthTx.transaction.SendResponse(unauthorized))

	bothAuthTx := waitTransaction(t, sipClient)
	require.Equal(t, sip.REGISTER, bothAuthTx.req.Method)
	require.NotNil(t, bothAuthTx.req.GetHeader("Proxy-Authorization"))
	require.NotNil(t, bothAuthTx.req.GetHeader("Authorization"))
	require.NoError(t, bothAuthTx.transaction.SendResponse(sip.NewResponseFromRequest(bothAuthTx.req, sip.StatusOK, "OK", nil)))
	require.NoError(t, <-done)
}

func TestEnsureRegisteredAddsRouteHeader(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)

	done := make(chan error, 1)
	go func() {
		_, err := client.ensureRegistered(context.Background(), sipOutboundConfig{
			address: mockRegistrarHost + ":5060",
			host:    "sip.telphin.com",
			user:    mockAuthUser,
			pass:    mockAuthPassword,
			headers: map[string]string{
				"Route": "<sip:sipproxy.telphin.ru:5068;lr>",
			},
		})
		done <- err
	}()

	tx := waitTransaction(t, sipClient)
	require.Equal(t, sip.REGISTER, tx.req.Method)
	routeHeaders := tx.req.GetHeaders("Route")
	require.Len(t, routeHeaders, 1)
	require.Equal(t, "<sip:sipproxy.telphin.ru:5068;lr>", routeHeaders[0].Value())
	require.NoError(t, tx.transaction.SendResponse(sip.NewResponseFromRequest(tx.req, sip.StatusOK, "OK", nil)))
	require.NoError(t, <-done)
}

func TestEnsureRegisteredAddsConfiguredRouteHeadersInOrder(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)

	routes := []string{
		"<sip:edge-1.example.com;lr>",
		"<sip:edge-2.example.com;lr>",
	}
	done := make(chan error, 1)
	go func() {
		_, err := client.ensureRegistered(context.Background(), sipOutboundConfig{
			address:      mockRegistrarHost + ":5060",
			host:         "sip.telphin.com",
			user:         mockAuthUser,
			pass:         mockAuthPassword,
			routeHeaders: routes,
		})
		done <- err
	}()

	tx := waitTransaction(t, sipClient)
	require.Equal(t, sip.REGISTER, tx.req.Method)
	routeHeaders := tx.req.GetHeaders("Route")
	require.Len(t, routeHeaders, 2)
	require.Equal(t, routes[0], routeHeaders[0].Value())
	require.Equal(t, routes[1], routeHeaders[1].Value())
	require.NoError(t, tx.transaction.SendResponse(sip.NewResponseFromRequest(tx.req, sip.StatusOK, "OK", nil)))
	require.NoError(t, <-done)
}

func TestEnsureRegisteredStoresServiceRouteHeaders(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)

	type result struct {
		conf *ResolvedRegistrationConfig
		err  error
	}
	done := make(chan result, 1)
	go func() {
		conf, err := client.ensureRegistered(context.Background(), sipOutboundConfig{
			address: mockRegistrarHost + ":5060",
			host:    "sip.beeline.example",
			user:    mockAuthUser,
			pass:    mockAuthPassword,
		})
		done <- result{conf: conf, err: err}
	}()

	tx := waitTransaction(t, sipClient)
	require.Equal(t, sip.REGISTER, tx.req.Method)
	resp := sip.NewResponseFromRequest(tx.req, sip.StatusOK, "OK", nil)
	resp.AppendHeader(sip.NewHeader("Service-Route", "<sip:ims-edge-1.example.com;lr>"))
	resp.AppendHeader(sip.NewHeader("Service-Route", "<sip:ims-edge-2.example.com;lr>"))
	require.NoError(t, tx.transaction.SendResponse(resp))

	res := <-done
	require.NoError(t, res.err)
	require.NotNil(t, res.conf)
	require.Equal(t, []string{
		"<sip:ims-edge-1.example.com;lr>",
		"<sip:ims-edge-2.example.com;lr>",
	}, res.conf.ServiceRouteHeaders)
}

func TestServiceRouteHeadersParsesBeelineRegisterResponse(t *testing.T) {
	raw := strings.Join([]string{
		"SIP/2.0 200 OK",
		"Via: SIP/2.0/UDP 81.29.140.248:15060;rport=15060;received=81.29.140.248;branch=z9hG4bK.1ykXqbaREIz80Ar5;alias",
		"To: <sip:9063671384@ip.beeline.ru>;tag=0a606b8e87cc800c51b22fb8c4896403-ffbd86ee",
		"From: <sip:9063671384@ip.beeline.ru>;tag=hifxE7HUUFztFsPI",
		"Call-ID: reg_kkfQxtaK4gfK",
		"CSeq: 2 REGISTER",
		"Contact:  <sip:9063671384@81.29.140.248:15060;transport=udp>;expires=150",
		"P-Associated-URI: <sip:9063671384@ip.beeline.ru>",
		"Server: ES-S-CSCF",
		"Content-Length: 0",
		"Service-Route: <sip:212.119.246.230:5060;transport=udp;lr;mpcftk=1-115-30c-8-4006a2a2>",
		"",
		"",
	}, "\r\n")

	msg, err := emisip.ParseMessage([]byte(raw))
	require.NoError(t, err)
	resp, ok := msg.(*sip.Response)
	require.True(t, ok)
	require.Equal(t, []string{
		"<sip:212.119.246.230:5060;transport=udp;lr;mpcftk=1-115-30c-8-4006a2a2>",
	}, serviceRouteHeaders(resp))
}

func TestEnsureRegisteredWithoutCredentialsSkipsRegister(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)

	conf, err := client.ensureRegistered(context.Background(), sipOutboundConfig{
		address: mockRegistrarHost + ":5060",
		user:    mockAuthUser,
	})
	require.NoError(t, err)
	require.Nil(t, conf)

	select {
	case tx := <-sipClient.transactions:
		t.Fatalf("unexpected REGISTER transaction: %v", tx.req.Method)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestResolveRegistrationConfigUsesCallDefaults(t *testing.T) {
	conf, err := resolveRegistrationConfig(sipOutboundConfig{
		address: mockRegistrarHost + ":5070",
		host:    "from.example.com",
		user:    mockAuthUser,
		pass:    mockAuthPassword,
	})
	require.NoError(t, err)
	require.NotNil(t, conf)
	require.Equal(t, "sip:registrar.example.com:5070", conf.RegistrarURI)
	require.Equal(t, "sip:registrar.example.com", conf.AuthURI)
	require.Equal(t, mockAuthUser, conf.AORUser)
	require.Equal(t, mockAuthUser, conf.AuthUsername)
	require.Equal(t, mockAuthUser, conf.ContactUser)
	require.Equal(t, "from.example.com", conf.FromDomain)
	require.Equal(t, TransportUDP, conf.Transport)
	require.Equal(t, defaultRegisterExpiration, conf.Expires)
	require.Equal(t, defaultRegisterRefresh, conf.RefreshBefore)
	require.False(t, conf.AlwaysRefreshBeforeInvite)
	require.True(t, conf.InviteOnRegisterFailure)
}

func TestResolveRegistrationConfigUsesRegistrarHostAsFromDomainFallback(t *testing.T) {
	conf, err := resolveRegistrationConfig(sipOutboundConfig{
		address: mockRegistrarHost + ":5060",
		user:    mockAuthUser,
		pass:    mockAuthPassword,
	})
	require.NoError(t, err)
	require.NotNil(t, conf)
	require.Equal(t, mockRegistrarHost, conf.FromDomain)
}

func TestResolveRegistrationConfigUsesTLSAuthURIDefault(t *testing.T) {
	conf, err := resolveRegistrationConfig(sipOutboundConfig{
		address:   mockRegistrarHost + ":5061",
		user:      mockAuthUser,
		pass:      mockAuthPassword,
		transport: livekit.SIPTransport_SIP_TRANSPORT_TLS,
	})
	require.NoError(t, err)
	require.NotNil(t, conf)
	require.Equal(t, TransportTLS, conf.Transport)
	require.Equal(t, "sip:registrar.example.com", conf.AuthURI)
}

func TestResolveRegistrationConfigRequiresUser(t *testing.T) {
	conf, err := resolveRegistrationConfig(sipOutboundConfig{
		address: mockRegistrarHost + ":5060",
		pass:    mockAuthPassword,
	})
	require.Nil(t, conf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "auth username")
}

func TestEnsureRegisteredCachesSuccessfulRegistration(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)

	done := make(chan error, 1)
	go func() {
		_, err := client.ensureRegistered(context.Background(), sipOutboundConfig{
			address: mockRegistrarHost + ":5060",
			host:    mockRegistrarHost,
			user:    mockAuthUser,
			pass:    mockAuthPassword,
		})
		done <- err
	}()
	tx := waitTransaction(t, sipClient)
	ok := sip.NewResponseFromRequest(tx.req, sip.StatusOK, "OK", nil)
	ok.AppendHeader(sip.NewHeader("Service-Route", "<sip:ims-edge.example.com;lr>"))
	require.NoError(t, tx.transaction.SendResponse(ok))
	require.NoError(t, <-done)

	conf, err := client.ensureRegistered(context.Background(), sipOutboundConfig{
		address: mockRegistrarHost + ":5060",
		host:    mockRegistrarHost,
		user:    mockAuthUser,
		pass:    mockAuthPassword,
	})
	require.NoError(t, err)
	require.NotNil(t, conf)
	require.Equal(t, []string{"<sip:ims-edge.example.com;lr>"}, conf.ServiceRouteHeaders)

	select {
	case tx = <-sipClient.transactions:
		t.Fatalf("unexpected extra REGISTER transaction: %v", tx.req.Method)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestForceRegisterBypassesCachedRegistration(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)

	conf, err := resolveRegistrationConfig(sipOutboundConfig{
		address: mockRegistrarHost + ":5060",
		host:    mockRegistrarHost,
		user:    mockAuthUser,
		pass:    mockAuthPassword,
	})
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		done <- client.forceRegister(context.Background(), conf, mockAuthPassword)
	}()
	tx := waitTransaction(t, sipClient)
	require.Equal(t, sip.REGISTER, tx.req.Method)
	require.NoError(t, tx.transaction.SendResponse(sip.NewResponseFromRequest(tx.req, sip.StatusOK, "OK", nil)))
	require.NoError(t, <-done)

	done = make(chan error, 1)
	go func() {
		done <- client.forceRegister(context.Background(), conf, mockAuthPassword)
	}()
	tx = waitTransaction(t, sipClient)
	require.Equal(t, sip.REGISTER, tx.req.Method)
	require.NoError(t, tx.transaction.SendResponse(sip.NewResponseFromRequest(tx.req, sip.StatusOK, "OK", nil)))
	require.NoError(t, <-done)
}

func TestEnsureRegisteredUsesResponseExpires(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)

	done := make(chan error, 1)
	go func() {
		_, err := client.ensureRegistered(context.Background(), sipOutboundConfig{
			address: mockRegistrarHost + ":5060",
			host:    mockRegistrarHost,
			user:    mockAuthUser,
			pass:    mockAuthPassword,
		})
		done <- err
	}()
	tx := waitTransaction(t, sipClient)
	ok := sip.NewResponseFromRequest(tx.req, sip.StatusOK, "OK", nil)
	ok.AppendHeader(sip.NewHeader("Expires", "20"))
	require.NoError(t, tx.transaction.SendResponse(ok))
	require.NoError(t, <-done)

	done = make(chan error, 1)
	go func() {
		_, err := client.ensureRegistered(context.Background(), sipOutboundConfig{
			address: mockRegistrarHost + ":5060",
			host:    mockRegistrarHost,
			user:    mockAuthUser,
			pass:    mockAuthPassword,
		})
		done <- err
	}()
	tx = waitTransaction(t, sipClient)
	require.Equal(t, sip.REGISTER, tx.req.Method)
	require.NoError(t, tx.transaction.SendResponse(sip.NewResponseFromRequest(tx.req, sip.StatusOK, "OK", nil)))
	require.NoError(t, <-done)
}

func TestRegistrationExpiresPrefersContactExpires(t *testing.T) {
	req := sip.NewRequest(sip.REGISTER, sip.Uri{Host: mockRegistrarHost})
	resp := sip.NewResponseFromRequest(req, sip.StatusOK, "OK", nil)
	resp.AppendHeader(sip.NewHeader("Expires", "120"))
	contact := &sip.ContactHeader{
		Address: sip.Uri{User: mockAuthUser, Host: "example.com"},
		Params:  sip.NewParams(),
	}
	contact.Params.Add("expires", "20")
	resp.AppendHeader(contact)

	require.Equal(t, 20*time.Second, registrationExpires(resp, defaultRegisterExpiration))
}

func TestRegistrationExpiresAppliesMinimumTTL(t *testing.T) {
	req := sip.NewRequest(sip.REGISTER, sip.Uri{Host: mockRegistrarHost})
	resp := sip.NewResponseFromRequest(req, sip.StatusOK, "OK", nil)
	resp.AppendHeader(sip.NewHeader("Expires", "1"))
	require.Equal(t, minRegisterExpiration, registrationExpires(resp, defaultRegisterExpiration))
}

func TestRegistrationBackgroundRefreshDoesNotSpinOnTinyExpiration(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)

	done := make(chan error, 1)
	go func() {
		_, err := client.ensureRegistered(context.Background(), sipOutboundConfig{
			address: mockRegistrarHost + ":5060",
			host:    mockRegistrarHost,
			user:    mockAuthUser,
			pass:    mockAuthPassword,
		})
		done <- err
	}()
	tx := waitTransaction(t, sipClient)
	ok := sip.NewResponseFromRequest(tx.req, sip.StatusOK, "OK", nil)
	ok.AppendHeader(sip.NewHeader("Expires", "1"))
	require.NoError(t, tx.transaction.SendResponse(ok))
	require.NoError(t, <-done)

	select {
	case tx = <-sipClient.transactions:
		t.Fatalf("unexpected immediate REGISTER refresh transaction: %v", tx.req.Method)
	case <-time.After(2 * time.Second):
	}
}

func TestRegistrationBackgroundRefreshRunsThirtySecondsBeforeExpiration(t *testing.T) {
	conf, err := resolveRegistrationConfig(sipOutboundConfig{
		address: mockRegistrarHost + ":5060",
		host:    mockRegistrarHost,
		user:    mockAuthUser,
		pass:    mockAuthPassword,
	})
	require.NoError(t, err)

	m := NewRegistrationManager()
	key := conf.cacheKey()
	now := time.Now()
	m.states[key] = &registrationState{
		expiresAt:  now.Add(150 * time.Second),
		lastUsedAt: now,
	}

	wait, stop := m.nextRefreshWait(key, conf)
	require.False(t, stop)
	require.GreaterOrEqual(t, wait, 119*time.Second)
	require.LessOrEqual(t, wait, 120*time.Second)
}

func TestEnsureRegisteredStoresSuccessfulRegisterTime(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)

	done := make(chan error, 1)
	go func() {
		_, err := client.ensureRegistered(context.Background(), sipOutboundConfig{
			address: mockRegistrarHost + ":5060",
			host:    mockRegistrarHost,
			user:    mockAuthUser,
			pass:    mockAuthPassword,
		})
		done <- err
	}()

	tx := waitTransaction(t, sipClient)
	ok := sip.NewResponseFromRequest(tx.req, sip.StatusOK, "OK", nil)
	require.NoError(t, tx.transaction.SendResponse(ok))
	require.NoError(t, <-done)

	conf, err := resolveRegistrationConfig(sipOutboundConfig{
		address: mockRegistrarHost + ":5060",
		host:    mockRegistrarHost,
		user:    mockAuthUser,
		pass:    mockAuthPassword,
	})
	require.NoError(t, err)

	age, fresh := client.registrationManager.freshSuccessfulRegisterAge(conf.cacheKey(), time.Minute)
	require.True(t, fresh)
	require.GreaterOrEqual(t, age, time.Duration(0))
	require.Less(t, age, time.Minute)
}

func TestEnsureRegisteredEnabledConfigDoesNotTreat405AsSkip(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)

	done := make(chan error, 1)
	go func() {
		_, err := client.ensureRegistered(context.Background(), sipOutboundConfig{
			address: mockRegistrarHost + ":5060",
			host:    mockRegistrarHost,
			user:    mockAuthUser,
			pass:    mockAuthPassword,
		})
		done <- err
	}()

	tx := waitTransaction(t, sipClient)
	resp := sip.NewResponseFromRequest(tx.req, sip.StatusMethodNotAllowed, "Method Not Allowed", nil)
	require.NoError(t, tx.transaction.SendResponse(resp))
	require.Error(t, <-done)
}

func getCreatedSIPClient(t *testing.T) *testSIPClient {
	t.Helper()
	select {
	case sipClient := <-createdClients:
		t.Cleanup(func() { _ = sipClient.Close() })
		return sipClient
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected test SIP client to be created")
		return nil
	}
}

func waitTransaction(t *testing.T, sipClient *testSIPClient) *transactionRequest {
	return waitTransactionWithTimeout(t, sipClient, 2*time.Second)
}

func waitTransactionWithTimeout(t *testing.T, sipClient *testSIPClient, timeout time.Duration) *transactionRequest {
	t.Helper()
	select {
	case tx := <-sipClient.transactions:
		t.Cleanup(func() { tx.transaction.Terminate() })
		return tx
	case <-time.After(timeout):
		t.Fatal("expected SIP transaction")
		return nil
	}
}
