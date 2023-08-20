package jamcli

import (
	"fmt"
	"log"
	"os"

	"github.com/zdgeier/jam/gen/jampb"
	"github.com/zdgeier/jam/pkg/jamcli/authfile"
	"github.com/zdgeier/jam/pkg/jamcli/statefile"
	"github.com/zdgeier/jam/pkg/jamgrpc"
	"golang.org/x/oauth2"
)

func Push() {
	authFile, err := authfile.Authorize()
	if err != nil {
		panic(err)
	}

	stateFile, err := statefile.Find()
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

	if stateFile.CommitInfo != nil {
		fmt.Println("Currently on the mainline, use `jam workon <workspace name>` to make changes.")
		os.Exit(1)
	}
	apiClient := jampb.NewJamHubClient(conn)

	fileMetadata := ReadLocalFileList()
	localToRemoteDiff, err := DiffLocalToRemoteWorkspace(apiClient, stateFile.OwnerUsername, stateFile.ProjectId, stateFile.WorkspaceInfo.WorkspaceId, stateFile.WorkspaceInfo.ChangeId, fileMetadata)
	if err != nil {
		log.Panic(err)
	}

	changeId := stateFile.WorkspaceInfo.ChangeId
	if DiffHasChanges(localToRemoteDiff) {
		err = pushFileListDiffWorkspace(conn, stateFile.OwnerUsername, stateFile.ProjectId, stateFile.WorkspaceInfo.WorkspaceId, changeId, fileMetadata, localToRemoteDiff)
		if err != nil {
			log.Panic(err)
		}
		for key, val := range localToRemoteDiff.GetDiffs() {
			if val.Type != jampb.FileMetadataDiff_NoOp {
				fmt.Println("Pushed", key)
			}
		}
		changeId += 1
	} else {
		fmt.Println("No changes to push")
	}

	err = statefile.StateFile{
		OwnerUsername: stateFile.OwnerUsername,
		ProjectId:     stateFile.ProjectId,
		WorkspaceInfo: &statefile.WorkspaceInfo{
			WorkspaceId: stateFile.WorkspaceInfo.WorkspaceId,
			ChangeId:    changeId,
		},
	}.Save()
	if err != nil {
		panic(err)
	}
}
