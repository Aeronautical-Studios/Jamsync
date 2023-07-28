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
		return Prod
	}
}
