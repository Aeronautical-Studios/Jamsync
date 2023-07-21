package jamhubgrpc

import (
	"crypto/tls"
	"crypto/x509"
	"embed"
	"log"
	"net"
	"path/filepath"

	"github.com/zdgeier/jamhub/gen/pb"
	"github.com/zdgeier/jamhub/internal/jamenv"
	"github.com/zdgeier/jamhub/internal/jamhub/changestore"
	"github.com/zdgeier/jamhub/internal/jamhub/db"
	"github.com/zdgeier/jamhub/internal/jamhub/opdatastorecommit"
	"github.com/zdgeier/jamhub/internal/jamhub/opdatastoreworkspace"
	"github.com/zdgeier/jamhub/internal/jamhub/oplocstorecommit"
	"github.com/zdgeier/jamhub/internal/jamhub/oplocstoreworkspace"
	"github.com/zdgeier/jamhub/internal/jamhubgrpc/serverauth"
	"golang.org/x/crypto/acme/autocert"
	"golang.org/x/oauth2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/oauth"
	"google.golang.org/grpc/reflection"
)

// //go:embed clientkey.pem
// var prodF embed.FS

//go:embed devclientkey.cer
var devF embed.FS

type JamHub struct {
	db                   db.JamHubDb
	opdatastoreworkspace *opdatastoreworkspace.LocalStore
	opdatastorecommit    *opdatastorecommit.LocalStore
	oplocstoreworkspace  *oplocstoreworkspace.LocalOpLocStore
	oplocstorecommit     *oplocstorecommit.LocalOpLocStore
	changestore          changestore.LocalChangeStore
	pb.UnimplementedJamHubServer
}

func New() (closer func(), err error) {
	jamhub := JamHub{
		db:                   db.New(),
		opdatastoreworkspace: opdatastoreworkspace.NewOpDataStoreWorkspace(),
		opdatastorecommit:    opdatastorecommit.NewOpDataStoreCommit(),
		oplocstoreworkspace:  oplocstoreworkspace.NewOpLocStoreWorkspace(),
		oplocstorecommit:     oplocstorecommit.NewOpLocStoreCommit(),
		changestore:          changestore.NewLocalChangeStore(),
	}

	opts := []grpc.ServerOption{
		grpc.UnaryInterceptor(serverauth.EnsureValidToken),
	}

	// var cert tls.Certificate
	// if jamenv.Env() == jamenv.Prod {
	// 	certManager := autocert.Manager{
	// 		Prompt:     autocert.AcceptTOS,
	// 		Cache:      autocert.DirCache("/etc/jamhub/certs"),
	// 		HostPolicy: autocert.HostWhitelist("grpc.jamhub.dev"),
	// 		Email:      "help@jamhub.dev",
	// 	}
	// 	// cert, err = tls.LoadX509KeyPair("/etc/jamsync/fullchain.pem", "/etc/jamsync/privkey.pem")
	// 	opts = append(opts, grpc.Creds(credentials.NewTLS(&tls.Config{GetCertificate: certManager.GetCertificate})))
	// } else {
	var creds credentials.TransportCredentials
	if jamenv.Env() == jamenv.Prod {
		manager := autocert.Manager{
			Prompt:     autocert.AcceptTOS,
			Cache:      autocert.DirCache("/etc/jamhub/certs"),
			HostPolicy: autocert.HostWhitelist("us-east-2-prod-jamhubgrpc.jamhub.dev"),
			Email:      "certs@jamhub.dev",
		}

		creds = credentials.NewTLS(manager.TLSConfig())
	} else {
		cert, err := tls.LoadX509KeyPair(filepath.Clean("x509/publickey.cer"), filepath.Clean("x509/private.key"))
		if err != nil {
			return nil, err
		}
		creds = credentials.NewServerTLSFromCert(&cert)
	}

	opts = append(opts, grpc.Creds(creds))

	server := grpc.NewServer(opts...)
	reflection.Register(server)
	pb.RegisterJamHubServer(server, jamhub)

	address := "0.0.0.0:14357"
	if jamenv.Env() == jamenv.Prod {
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

func Connect(accessToken *oauth2.Token) (client pb.JamHubClient, closer func(), err error) {
	opts := []grpc.DialOption{
		// grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
		// 	raddr, err := net.ResolveTCPAddr("tcp", addr)
		// 	if err != nil {
		// 		return nil, err
		// 	}

		// 	conn, err := net.DialTCP("tcp", nil, raddr)
		// 	if err != nil {
		// 		return nil, err
		// 	}

		// 	return conn, err
		// }),
	}
	// if jamenv.Env() != jamenv.Local {
	perRPC := oauth.TokenSource{TokenSource: oauth2.StaticTokenSource(accessToken)}
	opts = append(opts, grpc.WithPerRPCCredentials(perRPC))
	// }
	// var creds credentials.TransportCredentials
	// if jamenv.Env() == jamenv.Prod {
	// 	cp := x509.NewCertPool()
	// 	certData, err := prodF.ReadFile("clientkey.pem")
	// 	if err != nil {
	// 		return nil, nil, err
	// 	}
	// 	cp.AppendCertsFromPEM(certData)
	// 	creds = credentials.NewClientTLSFromCert(cp, "jamsync.dev")
	// } else {
	var creds credentials.TransportCredentials
	if jamenv.Env() == jamenv.Prod {
		config := &tls.Config{}
		creds = credentials.NewTLS(config)
	} else {
		cp := x509.NewCertPool()
		certData, err := devF.ReadFile("devclientkey.cer")
		if err != nil {
			return nil, nil, err
		}
		cp.AppendCertsFromPEM(certData)
		creds = credentials.NewClientTLSFromCert(cp, "jamsync.dev")
	}

	opts = append(opts, grpc.WithTransportCredentials(creds))

	addr := "0.0.0.0:14357"
	if jamenv.Env() == jamenv.Prod {
		addr = "us-east-2-prod-jamhubgrpc.jamhub.dev:443"
	}

	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		log.Panicf("could not connect to jamhub server: %s", err)
	}
	client = pb.NewJamHubClient(conn)
	closer = func() {
		if err := conn.Close(); err != nil {
			log.Panic("could not close server connection")
		}
	}

	return client, closer, err
}
