package jamcli

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/zdgeier/jam/gen/pb"
	"github.com/zdgeier/jam/pkg/jamcli/authfile"
	"github.com/zdgeier/jam/pkg/jamcli/statefile"
	"github.com/zdgeier/jam/pkg/jamgrpc"
	"golang.org/x/oauth2"
)

func WorkOn() {
	if len(os.Args) != 3 {
		fmt.Println("jam workon <workspace name>")
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

	if state.CommitInfo == nil || state.WorkspaceInfo != nil {
		// on commit
		if os.Args[2] == "main" || os.Args[2] == "mainline" {
			fileMetadata := ReadLocalFileList()
			localToRemoteDiff, err := DiffLocalToRemoteWorkspace(apiClient, state.OwnerUsername, state.ProjectId, state.WorkspaceInfo.WorkspaceId, state.WorkspaceInfo.ChangeId, fileMetadata)
			if err != nil {
				log.Panic(err)
			}
			if DiffHasChanges(localToRemoteDiff) {
				fmt.Println("Some changes locally have not been pushed. Run `jam push` to push your local changes.")
				os.Exit(1)
			}

			commitResp, err := apiClient.GetProjectCurrentCommit(context.Background(), &pb.GetProjectCurrentCommitRequest{
				OwnerUsername: state.OwnerUsername,
				ProjectId:     state.ProjectId,
			})
			if err != nil {
				log.Panic(err)
			}

			diffRemoteToLocalResp, err := DiffRemoteToLocalCommit(apiClient, state.OwnerUsername, state.ProjectId, commitResp.CommitId, fileMetadata)
			if err != nil {
				log.Panic(err)
			}

			err = ApplyFileListDiffCommit(apiClient, state.OwnerUsername, state.ProjectId, commitResp.CommitId, diffRemoteToLocalResp)
			if err != nil {
				log.Panic(err)
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
			return
		} else {
			fmt.Println("Must be on mainline to `workon` a new workspace.")
			os.Exit(1)
		}
	}

	// on mainline

	if os.Args[2] == "main" || os.Args[2] == "mainline" {
		fmt.Println("Already on `main`.")
		os.Exit(1)
	}

	resp, err := apiClient.ListWorkspaces(ctx, &pb.ListWorkspacesRequest{OwnerUsername: state.OwnerUsername, ProjectId: state.ProjectId})
	if err != nil {
		panic(err)
	}

	if workspaceId, ok := resp.GetWorkspaces()[os.Args[2]]; ok {
		if state.WorkspaceInfo != nil && workspaceId == state.WorkspaceInfo.WorkspaceId {
			fmt.Println("Already on", os.Args[2])
			return
		}

		// Check to see if there are any local changes that haven't been pushed
		fileMetadata := ReadLocalFileList()
		localToRemoteDiff, err := diffLocalToRemoteCommit(apiClient, state.OwnerUsername, state.ProjectId, state.CommitInfo.CommitId, fileMetadata)
		if err != nil {
			log.Panic(err)
		}
		if DiffHasChanges(localToRemoteDiff) {
			fmt.Println("Some changes have occurred on the `mainline` that have not been pushed. Run `jam workon` and `jam push` to save your local changes.")
			os.Exit(1)
		}

		changeResp, err := apiClient.GetWorkspaceCurrentChange(context.TODO(), &pb.GetWorkspaceCurrentChangeRequest{OwnerUsername: state.OwnerUsername, ProjectId: state.ProjectId, WorkspaceId: workspaceId})
		if err != nil {
			panic(err)
		}

		// if workspace already exists, do a pull
		remoteToLocalDiff, err := DiffRemoteToLocalWorkspace(apiClient, state.OwnerUsername, state.ProjectId, workspaceId, changeResp.ChangeId, fileMetadata)
		if err != nil {
			log.Panic(err)
		}

		if DiffHasChanges(remoteToLocalDiff) {
			err = ApplyFileListDiffWorkspace(apiClient, state.OwnerUsername, state.ProjectId, workspaceId, changeResp.ChangeId, remoteToLocalDiff)
			if err != nil {
				log.Panic(err)
			}
			for key, val := range remoteToLocalDiff.GetDiffs() {
				if val.Type != pb.FileMetadataDiff_NoOp {
					fmt.Println("Pulled", key)
				}
			}
		}

		err = statefile.StateFile{
			OwnerUsername: state.OwnerUsername,
			ProjectId:     state.ProjectId,
			WorkspaceInfo: &statefile.WorkspaceInfo{
				WorkspaceId: workspaceId,
				ChangeId:    changeResp.ChangeId,
			},
		}.Save()
		if err != nil {
			panic(err)
		}
	} else {
		// otherwise, just create a new workspace
		resp, err := apiClient.CreateWorkspace(ctx, &pb.CreateWorkspaceRequest{OwnerUsername: state.OwnerUsername, ProjectId: state.ProjectId, WorkspaceName: os.Args[2]})
		if err != nil {
			log.Panic(err)
		}

		err = statefile.StateFile{
			OwnerUsername: state.OwnerUsername,
			ProjectId:     state.ProjectId,
			WorkspaceInfo: &statefile.WorkspaceInfo{
				WorkspaceId: resp.WorkspaceId,
			},
		}.Save()
		if err != nil {
			panic(err)
		}
		fmt.Println("Switched to new workspace", os.Args[2]+".")
	}
}
