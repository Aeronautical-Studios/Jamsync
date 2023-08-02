package jamcli

import (
	"context"
	"fmt"
	"log"

	"github.com/zdgeier/jam/gen/pb"
	"github.com/zdgeier/jam/pkg/jamcli/authfile"
	"github.com/zdgeier/jam/pkg/jamcli/statefile"
	"github.com/zdgeier/jam/pkg/jamgrpc"
	"golang.org/x/oauth2"
)

func Pull() {
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
		log.Panic(err)
	}
	defer closer()

	if state.CommitInfo == nil {
		changeResp, err := apiClient.GetWorkspaceCurrentChange(context.Background(), &pb.GetWorkspaceCurrentChangeRequest{OwnerUsername: state.OwnerUsername, ProjectId: state.ProjectId, WorkspaceId: state.WorkspaceInfo.WorkspaceId})
		if err != nil {
			panic(err)
		}

		fileMetadata := ReadLocalFileList()
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
	} else {
		commitResp, err := apiClient.GetProjectCurrentCommit(context.Background(), &pb.GetProjectCurrentCommitRequest{OwnerUsername: state.OwnerUsername, ProjectId: state.ProjectId})
		if err != nil {
			panic(err)
		}

		fileMetadata := ReadLocalFileList()
		remoteToLocalDiff, err := DiffRemoteToLocalCommit(apiClient, state.OwnerUsername, state.ProjectId, commitResp.CommitId, fileMetadata)
		if err != nil {
			log.Panic(err)
		}

		if DiffHasChanges(remoteToLocalDiff) {
			err = ApplyFileListDiffCommit(apiClient, state.OwnerUsername, state.ProjectId, commitResp.CommitId, remoteToLocalDiff)
			if err != nil {
				log.Panic(err)
			}
			for key, val := range remoteToLocalDiff.GetDiffs() {
				if val.Type != pb.FileMetadataDiff_NoOp {
					fmt.Println("Pulled", key)
				}
			}
		} else {
			fmt.Println("No commits to pull")
		}

		err = statefile.StateFile{
			OwnerUsername: state.OwnerUsername,
			ProjectId:     state.ProjectId,
			CommitInfo: &statefile.CommitInfo{
				CommitId: commitResp.CommitId,
			},
		}.Save()
		if err != nil {
			panic(err)
		}
	}
}
