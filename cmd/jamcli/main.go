package main

import (
	"flag"
	"os"
	"runtime/pprof"
	"runtime/trace"

	"github.com/zdgeier/jam/pkg/jamcli"
)

var (
	version string
	built   string
)

func main() {
	f, err := os.Create("profile.prof")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// Start CPU profiling
	if err := pprof.StartCPUProfile(f); err != nil {
		panic(err)
	}
	defer pprof.StopCPUProfile()

	// Start tracing
	traceFile, err := os.Create("trace.out")
	if err != nil {
		panic(err)
	}
	defer traceFile.Close()

	if err := trace.Start(traceFile); err != nil {
		panic(err)
	}
	defer trace.Stop()

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
	case os.Args[1] == "workspaces":
		jamcli.ListWorkspaces()
	case os.Args[1] == "projects":
		jamcli.ListProjects()
	case os.Args[1] == "logout":
		jamcli.Logout()
	default:
		jamcli.Help(version, built)
	}
}
