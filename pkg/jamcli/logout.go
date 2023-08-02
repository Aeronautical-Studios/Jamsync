package jamcli

import (
	"fmt"

	"github.com/zdgeier/jam/pkg/jamcli/authfile"
)

func Logout() {
	authfile.Logout()
	fmt.Println("~/.jamhubauth file removed. Run `jam login` to log in.")
}
