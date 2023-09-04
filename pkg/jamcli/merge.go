package jamcli

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/zdgeier/jam/gen/jampb"
	"github.com/zdgeier/jam/pkg/jamcli/authfile"
	"github.com/zdgeier/jam/pkg/jamcli/statefile"
	"github.com/zdgeier/jam/pkg/jamgrpc"
	"golang.org/x/oauth2"
)

func Merge() {
	state, err := statefile.Find()
	if err != nil {
		fmt.Println("Could not find a `.jam` file. Run `jam init` to initialize the project.")
		return
	}

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

	if state.CommitInfo != nil {
		fmt.Println("Currently on the mainline, use `workon` to make changes.")
		os.Exit(1)
	}

	fileMetadata := ReadLocalFileList()
	apiClient := jampb.NewJamHubClient(conn)
	remoteToLocalDiff, err := diffRemoteToLocalWorkspace(apiClient, state.OwnerUsername, state.ProjectId, state.WorkspaceInfo.WorkspaceId, state.WorkspaceInfo.ChangeId, fileMetadata)
	if err != nil {
		log.Panic(err)
	}

	if DiffHasChanges(remoteToLocalDiff) {
		fmt.Println("You currently have active changes. Run `jam push` to push your local changes.")
		return
	}

	mergeMessage := os.Args[2]
	resp, err := apiClient.MergeWorkspace(context.Background(), &jampb.MergeWorkspaceRequest{
		OwnerUsername: state.OwnerUsername,
		ProjectId:     state.ProjectId,
		WorkspaceId:   state.WorkspaceInfo.WorkspaceId,
		MergeMessage:  mergeMessage,
	})
	if err != nil {
		fmt.Println("Workspace is not up to date with latest mainline changes. Use `jam update` to update to the workspace.")
		return
	}
	fmt.Println("Created commit", resp.CommitId)
}
