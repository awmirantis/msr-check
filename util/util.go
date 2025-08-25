package util

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	r "gopkg.in/rethinkdb/rethinkdb-go.v6"
)

func GetTlsConfig() (*tls.Config, error) {
	ca, err := os.ReadFile("/ca/rethink/cert.pem")
	if err != nil {
		fmt.Errorf("could not load ca.pem: %s", err)
	}
	keyPair, err := tls.LoadX509KeyPair("/ca/rethink-client/cert.pem", "/ca/rethink-client/key.pem")
	if err != nil {
		return nil, fmt.Errorf("could not load TLS key pair: %s", err)
	}
	certPool := x509.NewCertPool()
	certPool.AppendCertsFromPEM(ca)
	return &tls.Config{
		RootCAs:      certPool,
		Certificates: []tls.Certificate{keyPair},
	}, nil
}

func GetSession(replicaID string) (*r.Session, error) {
	tlsConfig, err := GetTlsConfig()
	if err != nil {
		return nil, err
	}
	connectOpts := r.ConnectOpts{
		Address:   "dtr-rethinkdb-" + replicaID + ".dtr-ol",
		TLSConfig: tlsConfig,
	}
	return r.Connect(connectOpts)
}
