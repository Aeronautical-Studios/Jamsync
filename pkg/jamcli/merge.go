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

	apiClient, closer, err := jamgrpc.Connect(&oauth2.Token{
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
	remoteToLocalDiff, err := DiffRemoteToLocalWorkspace(apiClient, state.OwnerUsername, state.ProjectId, state.WorkspaceInfo.WorkspaceId, state.WorkspaceInfo.ChangeId, fileMetadata)
	if err != nil {
		log.Panic(err)
	}

	if DiffHasChanges(remoteToLocalDiff) {
		fmt.Println("You currently have active changes. Run `jam push` to push your local changes.")
		return
	}

	resp, err := apiClient.MergeWorkspace(context.Background(), &jampb.MergeWorkspaceRequest{
		OwnerUsername: state.OwnerUsername,
		ProjectId:     state.ProjectId,
		WorkspaceId:   state.WorkspaceInfo.WorkspaceId,
	})
	if err != nil {
		fmt.Println("Workspace is not up to date with latest mainline changes. Use `jam update` to update to the workspace.")
		return
	}

	_, err = apiClient.DeleteWorkspace(context.Background(), &jampb.DeleteWorkspaceRequest{
		OwnerUsername: state.OwnerUsername,
		ProjectId:     state.ProjectId,
		WorkspaceId:   state.WorkspaceInfo.WorkspaceId,
	})
	if err != nil {
		log.Panic(err)
	}

	err = statefile.StateFile{
		OwnerUsername: state.OwnerUsername,
		ProjectId:     state.ProjectId,
		CommitInfo: &statefile.CommitInfo{
			CommitId: resp.CommitId,
		},
	}.Save()
	if err != nil {
		log.Panic(err)
	}
}
