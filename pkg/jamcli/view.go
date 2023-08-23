package jamcli

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/zdgeier/jam/gen/jampb"
	"github.com/zdgeier/jam/pkg/jamcli/authfile"
	"github.com/zdgeier/jam/pkg/jamcli/statefile"
	"github.com/zdgeier/jam/pkg/jamgrpc"
	"golang.org/x/oauth2"
)

func View() {
	if len(os.Args) != 3 {
		fmt.Println("jam view <change/commit id>")
		return
	}

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
		log.Panic(err)
	}
	defer closer()

	a, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Panic(err)
	}

	apiClient := jampb.NewJamHubClient(conn)

	if state.CommitInfo == nil {
		requestedChangeId := uint64(a)
		changeResp, err := apiClient.GetWorkspaceCurrentChange(context.Background(), &jampb.GetWorkspaceCurrentChangeRequest{OwnerUsername: state.OwnerUsername, ProjectId: state.ProjectId, WorkspaceId: state.WorkspaceInfo.WorkspaceId})
		if err != nil {
			panic(err)
		}

		if changeResp.ChangeId < requestedChangeId {
			fmt.Println("Requested change ID greater than max change ID of workspace.")
			os.Exit(1)
		}

		fileMetadata := ReadLocalFileList()
		remoteToLocalDiff, err := diffRemoteToLocalWorkspace(apiClient, state.OwnerUsername, state.ProjectId, state.WorkspaceInfo.WorkspaceId, requestedChangeId, fileMetadata)
		if err != nil {
			log.Panic(err)
		}

		if DiffHasChanges(remoteToLocalDiff) {
			err = ApplyFileListDiffWorkspace(apiClient, state.OwnerUsername, state.ProjectId, state.WorkspaceInfo.WorkspaceId, requestedChangeId, remoteToLocalDiff)
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
		err = statefile.StateFile{
			OwnerUsername: state.OwnerUsername,
			ProjectId:     state.ProjectId,
			WorkspaceInfo: &statefile.WorkspaceInfo{
				WorkspaceId: state.WorkspaceInfo.WorkspaceId,
				ChangeId:    requestedChangeId,
			},
		}.Save()
		if err != nil {
			panic(err)
		}
	} else {
		requestedCommitId := uint64(a)
		commitResp, err := apiClient.GetProjectCurrentCommit(context.Background(), &jampb.GetProjectCurrentCommitRequest{OwnerUsername: state.OwnerUsername, ProjectId: state.ProjectId})
		if err != nil {
			panic(err)
		}
		if commitResp.CommitId < requestedCommitId {
			fmt.Println("Requested change ID greater than max change ID of workspace.")
			os.Exit(1)
		}

		fileMetadata := ReadLocalFileList()
		remoteToLocalDiff, err := diffRemoteToLocalCommit(apiClient, state.OwnerUsername, state.ProjectId, requestedCommitId, fileMetadata)
		if err != nil {
			log.Panic(err)
		}

		if DiffHasChanges(remoteToLocalDiff) {
			err = ApplyFileListDiffCommit(apiClient, state.OwnerUsername, state.ProjectId, requestedCommitId, remoteToLocalDiff)
			if err != nil {
				log.Panic(err)
			}
			for key, val := range remoteToLocalDiff.GetDiffs() {
				if val.Type != jampb.FileMetadataDiff_NoOp {
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
				CommitId: requestedCommitId,
			},
		}.Save()
		if err != nil {
			panic(err)
		}
	}
}
