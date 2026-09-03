package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func testCertificate(t *testing.T) (keyBase64, certBase64 string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
	}, &x509.Certificate{SerialNumber: big.NewInt(1)}, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	return base64.StdEncoding.EncodeToString(keyPEM), base64.StdEncoding.EncodeToString(certPEM)
}

func TestLoadConfigReadsListeningSSLCredentials(t *testing.T) {
	t.Setenv("SSL_KEY_BASE64", "encoded-key")
	t.Setenv("SSL_CRT_BASE64", "encoded-cert")
	config := loadConfig()
	if config.SSLKeyBase64 != "encoded-key" || config.SSLCertBase64 != "encoded-cert" {
		t.Fatalf("SSL config = (%q, %q)", config.SSLKeyBase64, config.SSLCertBase64)
	}
}

func TestTLSConfigFromBase64LoadsSelfSignedCertificate(t *testing.T) {
	keyBase64, certBase64 := testCertificate(t)
	config, err := tlsConfigFromBase64(keyBase64, certBase64)
	if err != nil {
		t.Fatalf("tlsConfigFromBase64: %v", err)
	}
	if config.MinVersion != tls.VersionTLS12 || len(config.Certificates) != 1 {
		t.Fatalf("unexpected TLS config: %#v", config)
	}

	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	server.TLS = config
	server.StartTLS()
	defer server.Close()
	client := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // self-signed test certificate
	}}
	response, err := client.Get(server.URL)
	if err != nil {
		t.Fatalf("HTTPS request with self-signed certificate: %v", err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusNoContent)
	}
}

func TestTLSConfigFromBase64RejectsInvalidCredentials(t *testing.T) {
	_, err := tlsConfigFromBase64("not-base64", base64.StdEncoding.EncodeToString([]byte("cert")))
	if err == nil || !strings.Contains(err.Error(), "decode SSL key") {
		t.Fatalf("error = %v, want key decode error", err)
	}
}
