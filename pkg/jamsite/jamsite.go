package jamsite

import (
	"os"

	"github.com/zdgeier/jam/pkg/jamenv"
)

type JamSite int

const (
	USEast2 JamSite = iota
	USWest2
	Office
	Local
)

func (e JamSite) String() string {
	switch e {
	case USEast2:
		return "us-east-2"
	case USWest2:
		return "us-west-2"
	case Office:
		return "office"
	case Local:
		return "local"
	default:
		panic("unknown JAM_ENV")
	}
}

func Site() JamSite {
	jamSiteString := os.Getenv("JAM_SITE")
	switch jamSiteString {
	case "us-east-2":
		return USEast2
	case "us-west-2":
		return USWest2
	case "office":
		return Office
	case "local":
		return Local
	default:
		return USEast2
	}
}

func Host() string {
	host := "https://jamhub.dev"
	if jamenv.Env() == jamenv.Local {
		host = "http://localhost"
	} else if jamenv.Env() == jamenv.Staging {
		host = "https://staging.jamhub.dev"
	} else if Site() == USWest2 {
		host = "https://west.jamhub.dev"
	}
	return host
}
