package jamcli

import (
	"github.com/zdgeier/jam/pkg/jamcli/authfile"
)

func Login() {
	// authfile.Logout()
	_, err := authfile.Authorize()
	if err != nil {
		panic(err)
	}
}
