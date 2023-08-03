package jamcli

import (
	"fmt"
	"os"

	"github.com/zdgeier/jam/pkg/jamenv"
	"github.com/zdgeier/jam/pkg/jamsite"
)

func Help(version string, built string) {
	fmt.Println()
	fmt.Println("Welcome to Jam!")
	fmt.Println("\nversion:  ", version)
	fmt.Println("built:    ", built)
	fmt.Println("env:      ", jamenv.Env().String())
	fmt.Println("site:     ", jamsite.Site().String())
	fmt.Println("\nlogin      - do this first. creates ~/.jamhubauth.")
	fmt.Println("init       - initialize a project in the current directory.")
	fmt.Println("open       - open the current project in the browser.")
	fmt.Println("status     - print information about the local state of the project.")
	fmt.Println("push       - push up local modifications to a workspace.")
	fmt.Println("pull       - pull down remote modifications to the mainline or workspace.")
	fmt.Println("workon     - create or download a workspace.")
	fmt.Println("view       - view a commit or change.")
	fmt.Println("merge      - merge a workspace into the mainline.")
	fmt.Println("update     - brings mainline changes into workspace.")
	fmt.Println("workspaces - list active workspaces.")
	fmt.Println("projects   - list your projects.")
	fmt.Println("logout     - deletes ~/.jamhubauth.")
	fmt.Println("help       - show this text")
	fmt.Println("\nHappy jammin'!")
	fmt.Println()
	os.Exit(0)
}
