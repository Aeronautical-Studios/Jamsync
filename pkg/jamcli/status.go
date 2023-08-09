package jamcli

import (
	"context"
	"fmt"
	"log"

	"github.com/zdgeier/jam/gen/jampb"
	"github.com/zdgeier/jam/pkg/jamcli/authfile"
	"github.com/zdgeier/jam/pkg/jamcli/statefile"
	"github.com/zdgeier/jam/pkg/jamgrpc"
	"golang.org/x/oauth2"
)

func Status() {
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

	nameResp, err := apiClient.GetProjectName(context.Background(), &jampb.GetProjectNameRequest{
		OwnerUsername: state.OwnerUsername,
		ProjectId:     state.ProjectId,
	})
	if err != nil {
		panic(err)
	}

	if state.WorkspaceInfo != nil {
		workspaceNameResp, err := apiClient.GetWorkspaceName(context.Background(), &jampb.GetWorkspaceNameRequest{
			ProjectId:     state.ProjectId,
			OwnerUsername: state.OwnerUsername,
			WorkspaceId:   state.WorkspaceInfo.WorkspaceId,
		})
		if err != nil {
			panic(err)
		}
		fmt.Printf(
			"Project:   %s\n"+
				"Workspace: %s\n"+
				"Change:    %d\n",
			nameResp.ProjectName,
			workspaceNameResp.GetWorkspaceName(),
			state.WorkspaceInfo.ChangeId,
		)

		changeResp, err := apiClient.GetWorkspaceCurrentChange(context.Background(), &jampb.GetWorkspaceCurrentChangeRequest{OwnerUsername: state.OwnerUsername, ProjectId: state.ProjectId, WorkspaceId: state.WorkspaceInfo.WorkspaceId})
		if err != nil {
			panic(err)
		}

		if changeResp.ChangeId == state.WorkspaceInfo.ChangeId {
			fileMetadata := ReadLocalFileList()
			localToRemoteDiff, err := DiffLocalToRemoteWorkspace(apiClient, state.OwnerUsername, state.ProjectId, state.WorkspaceInfo.WorkspaceId, state.WorkspaceInfo.ChangeId, fileMetadata)
			if err != nil {
				log.Panic(err)
			}

			if DiffHasChanges(localToRemoteDiff) {
				fmt.Println("\nModified files:")
				for path, diff := range localToRemoteDiff.Diffs {
					if diff.Type != jampb.FileMetadataDiff_NoOp {
						fmt.Println("  " + path)
					}
				}
			} else {
				fmt.Println("\nNo local or remote changes.")

			}
		} else if changeResp.ChangeId > state.WorkspaceInfo.ChangeId {
			fileMetadata := ReadLocalFileList()
			remoteToLocalDiff, err := DiffRemoteToLocalWorkspace(apiClient, state.OwnerUsername, state.ProjectId, state.WorkspaceInfo.WorkspaceId, state.WorkspaceInfo.ChangeId, fileMetadata)
			if err != nil {
				log.Panic(err)
			}

			for path := range remoteToLocalDiff.GetDiffs() {
				fmt.Println(path, "changed")
			}
		} else {
			log.Panic("invalid state: local change id greater than remote change id")
		}
	} else {
		fmt.Printf("Project: %s\n", nameResp.ProjectName)
		fmt.Printf("Commit:  %d\n", state.CommitInfo.CommitId)
	}
}
