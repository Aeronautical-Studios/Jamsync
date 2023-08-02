package jamsite

import (
	"os"
)

type JamSite int

const (
	USEast2 JamSite = iota
	USWest2
	Local
)

func (e JamSite) String() string {
	switch e {
	case USEast2:
		return "us-east-2"
	case USWest2:
		return "us-west-2"
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
	case "local":
		return Local
	default:
		return USEast2
	}
}
