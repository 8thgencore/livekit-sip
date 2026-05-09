package sip

import (
	"context"
	"testing"
	"time"

	"github.com/icholy/digest"
	"github.com/stretchr/testify/require"

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
