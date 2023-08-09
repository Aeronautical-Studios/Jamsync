package jamgrpc

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/zdgeier/jam/gen/jampb"
	"github.com/zdgeier/jam/pkg/jamenv"
	"github.com/zdgeier/jam/pkg/jamgrpc/serverauth"
	"github.com/zdgeier/jam/pkg/jamsite"
	"github.com/zdgeier/jam/pkg/jamstores/commitdatastore"
	"github.com/zdgeier/jam/pkg/jamstores/jamdb"
	"github.com/zdgeier/jam/pkg/jamstores/projectstore"
	"github.com/zdgeier/jam/pkg/jamstores/workspacedatastore"
	"golang.org/x/crypto/acme/autocert"
	"golang.org/x/oauth2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/oauth"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
)

type JamHub struct {
	db                 jamdb.LocalStore
	workspacedatastore *workspacedatastore.LocalStore
	commitdatastore    *commitdatastore.LocalStore
	projectstore       *projectstore.LocalStore
	jampb.UnimplementedJamHubServer
}

func Hostname() string {
	if jamenv.Env() == jamenv.Local {
		return "0.0.0.0:14357"
	}
	return jamsite.Site().String() + "-" + jamenv.Env().String() + "-jamhubgrpc.jamhub.dev:443"
}

func New() (closer func(), err error) {
	jamhub := JamHub{
		db:                 jamdb.NewLocalStore(),
		workspacedatastore: workspacedatastore.NewLocalStore(),
		commitdatastore:    commitdatastore.NewLocalStore(),
		projectstore:       projectstore.NewLocalStore(),
	}

	opts := []grpc.ServerOption{
		grpc.UnaryInterceptor(serverauth.EnsureValidToken),
	}

	var host string
	if jamenv.Env() == jamenv.Local {
		host = "jamhub.dev"
		serverCert, err := tls.LoadX509KeyPair("/etc/jamhub/certs/server-cert.pem", "/etc/jamhub/certs/server-key.pem")
		if err != nil {
			return nil, err
		}

		// Create the credentials and return it
		config := &tls.Config{
			Certificates: []tls.Certificate{serverCert},
			ClientAuth:   tls.NoClientCert,
		}

		opts = append(opts, grpc.Creds(credentials.NewTLS(config)))
	} else {
		if jamenv.Env() == jamenv.Staging {
			host = "us-west-2-staging-jamhubgrpc.jamhub.dev"
		} else {
			switch jamsite.Site() {
			case jamsite.USEast2:
				host = "us-east-2-prod-jamhubgrpc.jamhub.dev"
			case jamsite.USWest2:
				host = "us-west-2-prod-jamhubgrpc.jamhub.dev"
			}
		}
		manager := autocert.Manager{
			Prompt:     autocert.AcceptTOS,
			Cache:      autocert.DirCache("/etc/jamhub/certs"),
			HostPolicy: autocert.HostWhitelist(host),
			Email:      "certs@jamhub.dev",
		}

		opts = append(opts, grpc.Creds(credentials.NewTLS(manager.TLSConfig())))
	}

	server := grpc.NewServer(opts...)
	reflection.Register(server)
	jampb.RegisterJamHubServer(server, jamhub)

	address := "0.0.0.0:14357"
	if jamenv.Env() == jamenv.Prod || jamenv.Env() == jamenv.Staging {
		address = "0.0.0.0:443"
	}

	tcplis, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	go func() {
		if err := server.Serve(tcplis); err != nil {
			log.Printf("error serving server: %v", err)
		}
	}()

	return func() { server.Stop() }, nil
}

func Connect(accessToken *oauth2.Token) (client jampb.JamHubClient, closer func(), err error) {

	md := metadata.New(map[string]string{"content-type": "application/grpc"})
	perRPC := oauth.TokenSource{TokenSource: oauth2.StaticTokenSource(accessToken)}
	opts := []grpc.DialOption{grpc.WithPerRPCCredentials(perRPC), grpc.WithDefaultCallOptions(grpc.Header(&md))}

	if jamenv.Env() == jamenv.Local {
		pemServerCA, err := os.ReadFile("/etc/jamhub/certs/ca-cert.pem")
		if err != nil {
			return nil, nil, err
		}
		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(pemServerCA) {
			return nil, nil, fmt.Errorf("failed to add server CA's certificate")
		}

		// Create the credentials and return it
		config := &tls.Config{
			RootCAs: certPool,
		}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(config)))
	} else {
		config := &tls.Config{}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(config)))
	}

	conn, err := grpc.Dial(Hostname(), opts...)
	if err != nil {
		log.Panicf("could not connect to jamhub server: %s", err)
	}
	client = jampb.NewJamHubClient(conn)
	closer = func() {
		if err := conn.Close(); err != nil {
			log.Panic("could not close server connection")
		}
	}

	return client, closer, err
}
