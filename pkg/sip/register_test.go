package sip

import (
	"context"
	"testing"
	"time"

	"github.com/icholy/digest"
	"github.com/stretchr/testify/require"

	"github.com/livekit/sipgo/sip"
)

func TestEnsureRegisteredHandlesDigestChallenge(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := client.ensureRegistered(ctx, sipOutboundConfig{
			address: "registrar.example.com:5060",
			host:    "registrar.example.com",
			user:    "alice",
			pass:    "secret",
		})
		done <- err
	}()

	firstTx := waitTransaction(t, sipClient)
	require.Equal(t, sip.REGISTER, firstTx.req.Method)
	require.Nil(t, firstTx.req.GetHeader("Authorization"))

	challenge := digest.Challenge{
		Realm: "registrar.example.com",
		Nonce: "nonce-1",
	}
	unauthorized := sip.NewResponseFromRequest(firstTx.req, sip.StatusUnauthorized, "Unauthorized", nil)
	unauthorized.AppendHeader(sip.NewHeader("WWW-Authenticate", challenge.String()))
	require.NoError(t, firstTx.transaction.SendResponse(unauthorized))

	secondTx := waitTransaction(t, sipClient)
	require.Equal(t, sip.REGISTER, secondTx.req.Method)
	authHeader := secondTx.req.GetHeader("Authorization")
	require.NotNil(t, authHeader)
	require.Contains(t, authHeader.Value(), `username="alice"`)

	ok := sip.NewResponseFromRequest(secondTx.req, sip.StatusOK, "OK", nil)
	require.NoError(t, secondTx.transaction.SendResponse(ok))
	require.NoError(t, <-done)
}

func TestEnsureRegisteredWithoutCredentialsSkipsRegister(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)

	conf, err := client.ensureRegistered(context.Background(), sipOutboundConfig{
		address: "registrar.example.com:5060",
		user:    "alice",
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
		address: "registrar.example.com:5070",
		host:    "from.example.com",
		user:    "alice",
		pass:    "secret",
	})
	require.NoError(t, err)
	require.NotNil(t, conf)
	require.Equal(t, "sip:registrar.example.com:5070", conf.RegistrarURI)
	require.Equal(t, "sip:registrar.example.com", conf.AuthURI)
	require.Equal(t, "alice", conf.AORUser)
	require.Equal(t, "alice", conf.AuthUsername)
	require.Equal(t, "alice", conf.ContactUser)
	require.Equal(t, "from.example.com", conf.FromDomain)
	require.Equal(t, TransportUDP, conf.Transport)
	require.Equal(t, defaultRegisterExpiration, conf.Expires)
	require.Equal(t, defaultRegisterRefresh, conf.RefreshBefore)
	require.False(t, conf.AlwaysRefreshBeforeInvite)
	require.True(t, conf.InviteOnRegisterFailure)
}

func TestResolveRegistrationConfigUsesRegistrarHostAsFromDomainFallback(t *testing.T) {
	conf, err := resolveRegistrationConfig(sipOutboundConfig{
		address: "registrar.example.com:5060",
		user:    "alice",
		pass:    "secret",
	})
	require.NoError(t, err)
	require.NotNil(t, conf)
	require.Equal(t, "registrar.example.com", conf.FromDomain)
}

func TestResolveRegistrationConfigUsesTLSAuthURIDefault(t *testing.T) {
	conf, err := resolveRegistrationConfig(sipOutboundConfig{
		address:   "registrar.example.com:5061",
		user:      "alice",
		pass:      "secret",
		transport: 2,
	})
	require.NoError(t, err)
	require.NotNil(t, conf)
	require.Equal(t, TransportTLS, conf.Transport)
	require.Equal(t, "sip:registrar.example.com", conf.AuthURI)
}

func TestResolveRegistrationConfigRequiresUser(t *testing.T) {
	conf, err := resolveRegistrationConfig(sipOutboundConfig{
		address: "registrar.example.com:5060",
		pass:    "secret",
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
			address: "registrar.example.com:5060",
			host:    "registrar.example.com",
			user:    "alice",
			pass:    "secret",
		})
		done <- err
	}()
	tx := waitTransaction(t, sipClient)
	ok := sip.NewResponseFromRequest(tx.req, sip.StatusOK, "OK", nil)
	require.NoError(t, tx.transaction.SendResponse(ok))
	require.NoError(t, <-done)

	conf, err := client.ensureRegistered(context.Background(), sipOutboundConfig{
		address: "registrar.example.com:5060",
		host:    "registrar.example.com",
		user:    "alice",
		pass:    "secret",
	})
	require.NoError(t, err)
	require.NotNil(t, conf)

	select {
	case tx = <-sipClient.transactions:
		t.Fatalf("unexpected extra REGISTER transaction: %v", tx.req.Method)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestEnsureRegisteredEnabledConfigDoesNotTreat405AsSkip(t *testing.T) {
	client := NewOutboundTestClient(t, TestClientConfig{})
	sipClient := getCreatedSIPClient(t)

	done := make(chan error, 1)
	go func() {
		_, err := client.ensureRegistered(context.Background(), sipOutboundConfig{
			address: "registrar.example.com:5060",
			host:    "registrar.example.com",
			user:    "alice",
			pass:    "secret",
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
	t.Helper()
	select {
	case tx := <-sipClient.transactions:
		t.Cleanup(func() { tx.transaction.Terminate() })
		return tx
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected SIP transaction")
		return nil
	}
}
