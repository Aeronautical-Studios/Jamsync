package jamenv

import (
	"os"
)

type JamEnv int

const (
	Prod JamEnv = iota
	Staging
	Local
)

func (e JamEnv) String() string {
	switch e {
	case Prod:
		return "prod"
	case Staging:
		return "staging"
	case Local:
		return "local"
	}
	panic("unknown JAM_ENV")
}

func Env() JamEnv {
	jamEnvString := os.Getenv("JAM_ENV")
	switch jamEnvString {
	case "local":
		return Local
	case "staging":
		return Staging
	default:
		panic("JAM_ENV must be set to either `local`, `staging` or `prod`")
	}
}

type JamSite int

const (
	USEast2 JamSite = iota
	USWest2
)

func (e JamSite) String() string {
	switch e {
	case USEast2:
		return "us-east-2"
	case USWest2:
		return "us-west-2"
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
	default:
		panic("JAM_SITE must be set to either `us-east-2` or `us-west-2`")
	}
}
