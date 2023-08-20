package jamcli

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/zdgeier/jam/gen/jampb"
	"github.com/zdgeier/jam/pkg/jamcli/authfile"
	"github.com/zdgeier/jam/pkg/jamcli/statefile"
	"github.com/zdgeier/jam/pkg/jamgrpc"
	"golang.org/x/oauth2"
)

func ListWorkspaces() {
	authFile, err := authfile.Authorize()
	if err != nil {
		panic(err)
	}

	conn, closer, err := jamgrpc.Connect(&oauth2.Token{
		AccessToken: string(authFile.Token),
	})
	if err != nil {
		panic(err)
	}
	defer closer()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err != nil {
		panic(err)
	}

	state, err := statefile.Find()
	if err != nil {
		fmt.Println("Could not find a `.jam` file. Run `jam init` to initialize the project.")
		os.Exit(0)
	}

	apiClient := jampb.NewJamHubClient(conn)

	resp, err := apiClient.ListWorkspaces(ctx, &jampb.ListWorkspacesRequest{OwnerUsername: state.OwnerUsername, ProjectId: state.ProjectId})
	if err != nil {
		log.Panic(err)
	}

	for name := range resp.GetWorkspaces() {
		fmt.Println(name)
	}
}
