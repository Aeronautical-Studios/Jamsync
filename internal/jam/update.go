package jam

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/zdgeier/jamhub/gen/pb"
	"github.com/zdgeier/jamhub/internal/jam/authfile"
	"github.com/zdgeier/jamhub/internal/jam/statefile"
	"github.com/zdgeier/jamhub/internal/jamhubgrpc"
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

	apiClient, closer, err := jamhubgrpc.Connect(&oauth2.Token{
		AccessToken: string(authFile.Token),
	})
	if err != nil {
		log.Panic(err)
	}
	defer closer()

	if state.CommitInfo != nil {
		fmt.Println("Currently on the mainline, use `workon` to make changes.")
		os.Exit(1)
	}

	fileMetadata := ReadLocalFileList()
	localToRemoteDiff, err := DiffLocalToRemoteWorkspace(apiClient, state.OwnerUsername, state.ProjectId, state.WorkspaceInfo.WorkspaceId, state.WorkspaceInfo.ChangeId, fileMetadata)
	if err != nil {
		log.Panic(err)
	}

	if DiffHasChanges(localToRemoteDiff) {
		fmt.Println("Some changes locally have not been pushed. Run `jam push` to push your local changes.")
		return
	}

	_, err = apiClient.UpdateWorkspace(context.Background(), &pb.UpdateWorkspaceRequest{
		ProjectId:     state.ProjectId,
		WorkspaceId:   state.WorkspaceInfo.WorkspaceId,
		OwnerUsername: state.OwnerUsername,
	})
	if err != nil {
		log.Panic(err)
	}

	changeResp, err := apiClient.GetWorkspaceCurrentChange(context.Background(), &pb.GetWorkspaceCurrentChangeRequest{OwnerUsername: state.OwnerUsername, ProjectId: state.ProjectId, WorkspaceId: state.WorkspaceInfo.WorkspaceId})
	if err != nil {
		panic(err)
	}

	remoteToLocalDiff, err := DiffRemoteToLocalWorkspace(apiClient, state.OwnerUsername, state.ProjectId, state.WorkspaceInfo.WorkspaceId, changeResp.GetChangeId(), fileMetadata)
	if err != nil {
		log.Panic(err)
	}

	if DiffHasChanges(remoteToLocalDiff) {
		err = ApplyFileListDiffWorkspace(apiClient, state.OwnerUsername, state.ProjectId, state.WorkspaceInfo.WorkspaceId, changeResp.GetChangeId(), remoteToLocalDiff)
		if err != nil {
			log.Panic(err)
		}
		for key, val := range remoteToLocalDiff.GetDiffs() {
			if val.Type != pb.FileMetadataDiff_NoOp {
				fmt.Println("Pulled", key)
			}
		}
	} else {
		fmt.Println("No changes to pull")
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
