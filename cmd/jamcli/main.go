package main

import (
	"archive/zip"
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"runtime"

	"github.com/fynelabs/selfupdate"
	"github.com/zdgeier/jam/pkg/jamcli"
	"github.com/zdgeier/jam/pkg/jamenv"
	"github.com/zdgeier/jam/pkg/jamsite"
)

var (
	version string
	built   string
)

func main() {
	// Automatically update jam if there's a new version
	if jamenv.Env() != jamenv.Local {
		vresp, err := http.Get(jamsite.Host() + "/currentversion")
		if err != nil {
			panic(err)
		}
		defer vresp.Body.Close()

		vb, err := io.ReadAll(vresp.Body)
		if err != nil {
			log.Panic(err)
		}
		if string(vb) != version && (len(os.Args) == 1 || os.Args[1] != "upgrade") {
			fmt.Println("Looks like there's a new version. Updating to v" + string(vb) + ".")
			resp, err := http.Get(jamsite.Host() + "/clients/" + string(vb) + "/jam_" + runtime.GOOS + "_" + runtime.GOARCH + ".zip")
			if err != nil {
				panic(err)
			}
			defer resp.Body.Close()

			b, err := io.ReadAll(resp.Body)
			if err != nil {
				log.Panic(err)
			}

			reader := bytes.NewReader(b)
			zipReader, err := zip.NewReader(reader, int64(len(b)))
			if err != nil {
				panic(err)
			}

			f, err := zipReader.Open("jam")
			if err != nil {
				panic(err)
			}

			err = selfupdate.Apply(f, selfupdate.Options{})
			if err != nil {
				panic(err)
			}

			fmt.Println("Done. Back to jammin'!")
			os.Exit(1)
		}
	}

	flag.Parse()

	switch {
	case len(os.Args) == 1:
		jamcli.Help(version, built)
	case os.Args[1] == "login":
		jamcli.Login()
	case os.Args[1] == "init":
		jamcli.InitConfig()
	case os.Args[1] == "open":
		jamcli.Open()
	case os.Args[1] == "pull":
		jamcli.Pull()
	case os.Args[1] == "status":
		jamcli.Status()
	case os.Args[1] == "push":
		jamcli.Push()
	case os.Args[1] == "view":
		jamcli.View()
	case os.Args[1] == "merge":
		jamcli.Merge()
	case os.Args[1] == "update":
		jamcli.Update()
	case os.Args[1] == "lock":
		jamcli.Lock()
	case os.Args[1] == "unlock":
		jamcli.UnLock()
	case os.Args[1] == "workon":
		jamcli.WorkOn()
	case os.Args[1] == "log":
		jamcli.Log()
	case os.Args[1] == "workspaces":
		jamcli.ListWorkspaces()
	case os.Args[1] == "projects":
		jamcli.ListProjects()
	case os.Args[1] == "upgrade":
		jamcli.Upgrade()
	case os.Args[1] == "logout":
		jamcli.Logout()
	default:
		jamcli.Help(version, built)
	}
}

// f, err := os.Create("profile.prof")
// if err != nil {
// 	panic(err)
// }
// defer f.Close()

// // Start CPU profiling
// if err := pprof.StartCPUProfile(f); err != nil {
// 	panic(err)
// }
// defer pprof.StopCPUProfile()

// // Start tracing
// traceFile, err := os.Create("trace.out")
// if err != nil {
// 	panic(err)
// }
// defer traceFile.Close()

// if err := trace.Start(traceFile); err != nil {
// 	panic(err)
// }
// defer trace.Stop()
