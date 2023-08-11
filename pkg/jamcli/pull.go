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
	b64 "encoding/base64"
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
		changeResp, err := apiClient.GetWorkspaceCurrentChange(context.Background(), &jampb.GetWorkspaceCurrentChangeRequest{OwnerUsername: state.OwnerUsername, ProjectId: state.ProjectId, WorkspaceId: state.WorkspaceInfo.WorkspaceId})
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
				ChangeId:    changeResp.ChangeId,
			},
		}.Save()
		if err != nil {
			panic(err)
		}
	} else {
		commitResp, err := apiClient.GetProjectCurrentCommit(context.Background(), &jampb.GetProjectCurrentCommitRequest{OwnerUsername: state.OwnerUsername, ProjectId: state.ProjectId})
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
				CommitId: commitResp.CommitId,
			},
		}.Save()
		if err != nil {
			panic(err)
		}

		// handle incoming file locks
		lockedFiles, err := apiClient.ListFileLocks(context.Background(), &jampb.ListFileLocksRequest{
			OwnerUsername: state.OwnerUsername,
			ProjectId:     state.ProjectId,
		})

		if err != nil {
			panic(err)
		}

		// check lockedFiles for locks that are not owned by the current user and set them to read only
		for _, lock := range lockedFiles.GetLockedFiles() {
			if lock.GetOwnerUsername() != state.OwnerUsername {
				// set file to read only
				b64EncodedPath := lock.GetB64EncodedPath()
				filePath, err := b64.URLEncoding.DecodeString(b64EncodedPath)
				err = os.Chmod(string(filePath), 0444)
				if err != nil {
					panic(err)
				}
			}
		}
	}
}
