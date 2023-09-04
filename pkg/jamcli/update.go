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

func Update() {
	authFile, err := authfile.Authorize()
	if err != nil {
		panic(err)
	}

	state, err := statefile.Find()
	if err != nil {
		fmt.Println("Could not find a `.jam` file. Run `jam init` to initialize the project.")
		os.Exit(1)
	}

	conn, closer, err := jamgrpc.Connect(&oauth2.Token{
		AccessToken: string(authFile.Token),
	})
	if err != nil {
		log.Panic(err)
	}
	defer closer()

	apiClient := jampb.NewJamHubClient(conn)

	if state.CommitInfo != nil {
		fmt.Println("Currently on the mainline, use `workon` to update a workspace.")
		os.Exit(1)
	}

	fileMetadata := ReadLocalFileList()
	localToRemoteDiff, err := diffLocalToRemoteWorkspace(apiClient, state.OwnerUsername, state.ProjectId, state.WorkspaceInfo.WorkspaceId, state.WorkspaceInfo.ChangeId, fileMetadata)
	if err != nil {
		log.Panic(err)
	}

	if DiffHasChanges(localToRemoteDiff) {
		fmt.Println("Some changes locally have not been pushed. Run `jam push` to push your local changes.")
		return
	}

	updateResp, err := apiClient.UpdateWorkspace(context.Background(), &jampb.UpdateWorkspaceRequest{
		ProjectId:     state.ProjectId,
		WorkspaceId:   state.WorkspaceInfo.WorkspaceId,
		OwnerUsername: state.OwnerUsername,
	})
	if err != nil {
		log.Panic(err)
	}

	changeResp, err := apiClient.GetWorkspaceCurrentChange(context.Background(), &jampb.GetWorkspaceCurrentChangeRequest{OwnerUsername: state.OwnerUsername, ProjectId: state.ProjectId, WorkspaceId: state.WorkspaceInfo.WorkspaceId})
	if err != nil {
		panic(err)
	}

	remoteToLocalDiff, err := diffRemoteToLocalWorkspace(apiClient, state.OwnerUsername, state.ProjectId, state.WorkspaceInfo.WorkspaceId, changeResp.GetChangeId(), fileMetadata)
	if err != nil {
		log.Panic(err)
	}

	if DiffHasChanges(remoteToLocalDiff) {
		err = ApplyFileListDiffWorkspace(apiClient, state.OwnerUsername, state.ProjectId, state.WorkspaceInfo.WorkspaceId, changeResp.GetChangeId(), remoteToLocalDiff)
		if err != nil {
			log.Panic(err)
		}
		for key, val := range remoteToLocalDiff.GetDiffs() {
			if val.Type != jampb.FileMetadataDiff_NoOp {
				fmt.Println("Pulled", key)
			}
		}
	} else {
		fmt.Println("No changes to pull")
	}

	if len(updateResp.GetConflicts()) > 0 {
		fmt.Println("Conflicts:")
		for _, conflict := range updateResp.GetConflicts() {
			fmt.Println(conflict)
		}
		fmt.Println("Resolve these conflicts in an editor before running `jam push`.")
	}

	err = statefile.StateFile{
		OwnerUsername: state.OwnerUsername,
		ProjectId:     state.ProjectId,
		WorkspaceInfo: &statefile.WorkspaceInfo{
			WorkspaceId: state.WorkspaceInfo.WorkspaceId,
			ChangeId:    changeResp.ChangeId,
		},
	}.Save()
	if err != nil {
		panic(err)
	}
}
