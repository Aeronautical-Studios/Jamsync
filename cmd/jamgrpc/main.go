package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	_ "github.com/mattn/go-sqlite3"
	"github.com/zdgeier/jam/pkg/jamenv"
	"github.com/zdgeier/jam/pkg/jamgrpc"
	"github.com/zdgeier/jam/pkg/jamsite"
)

var (
	version string
	built   string
)

func main() {
	log.Println("version: " + version)
	log.Println("built:   " + built)
	log.Println("env:     " + jamenv.Env().String())
	log.Println("site:    " + jamsite.Site().String())
	closer, err := jamgrpc.New()
	if err != nil {
		log.Panic(err)
	}
	log.Println("JamHub server is running...")

	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	<-done

	log.Println("JamHub server is stopping...")

	closer()
}
