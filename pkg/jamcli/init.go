package jamcli

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/zdgeier/jam/gen/jampb"
	"github.com/zdgeier/jam/pkg/jamcli/authfile"
	"github.com/zdgeier/jam/pkg/jamcli/statefile"
	"github.com/zdgeier/jam/pkg/jamgrpc"
	"golang.org/x/oauth2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func InitNewProject(apiClient jampb.JamHubClient, projectName string) {
	resp, err := apiClient.AddProject(context.Background(), &jampb.AddProjectRequest{
		ProjectName: projectName,
	})
	if status.Code(err) == codes.AlreadyExists {
		fmt.Println("Project already exists")
		os.Exit(1)
	} else if err != nil {
		panic(err)
	}
	currentPath, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	fmt.Println("Initializing a project at " + currentPath + ". Uploading files...")

	workspaceResp, err := apiClient.CreateWorkspace(context.TODO(), &jampb.CreateWorkspaceRequest{OwnerUsername: resp.GetOwnerUsername(), ProjectId: resp.ProjectId, WorkspaceName: "init"})
	if err != nil {
		log.Panic(err)
	}

	fileMetadata := ReadLocalFileList()
	fileMetadataDiff, err := diffLocalToRemoteCommit(apiClient, resp.GetOwnerUsername(), resp.GetProjectId(), workspaceResp.GetWorkspaceId(), fileMetadata)
	if err != nil {
		panic(err)
	}

	err = pushFileListDiffWorkspace(apiClient, resp.GetOwnerUsername(), resp.GetProjectId(), workspaceResp.WorkspaceId, 0, fileMetadata, fileMetadataDiff)
	if err != nil {
		panic(err)
	}

	fmt.Println("Merging...")
	mergeResp, err := apiClient.MergeWorkspace(context.Background(), &jampb.MergeWorkspaceRequest{
		ProjectId:     resp.GetProjectId(),
		OwnerUsername: resp.GetOwnerUsername(),
		WorkspaceId:   workspaceResp.WorkspaceId,
	})
	if err != nil {
		panic(err)
	}

	_, err = apiClient.DeleteWorkspace(context.Background(), &jampb.DeleteWorkspaceRequest{
		OwnerUsername: resp.GetOwnerUsername(),
		ProjectId:     resp.GetProjectId(),
		WorkspaceId:   workspaceResp.WorkspaceId,
	})
	if err != nil {
		log.Panic(err)
	}

	err = statefile.StateFile{
		OwnerUsername: resp.OwnerUsername,
		ProjectId:     resp.ProjectId,
		CommitInfo: &statefile.CommitInfo{
			CommitId: mergeResp.CommitId,
		},
	}.Save()
	if err != nil {
		panic(err)
	}
	fmt.Println("Done! Run `jam workon <workspace name>` to start making changes.")
}

func InitExistingProject(apiClient jampb.JamHubClient, ownerUsername string, projectName string) {
	resp, err := apiClient.GetProjectId(context.Background(), &jampb.GetProjectIdRequest{
		OwnerUsername: ownerUsername,
		ProjectName:   projectName,
	})
	if err != nil {
		log.Panic(err)
	}

	commitResp, err := apiClient.GetProjectCurrentCommit(context.Background(), &jampb.GetProjectCurrentCommitRequest{
		OwnerUsername: ownerUsername,
		ProjectId:     resp.GetProjectId(),
	})
	if err != nil {
		log.Panic(err)
	}

	diffRemoteToLocalResp, err := DiffRemoteToLocalCommit(apiClient, ownerUsername, resp.ProjectId, commitResp.CommitId, &jampb.FileMetadata{})
	if err != nil {
		log.Panic(err)
	}

	err = ApplyFileListDiffCommit(apiClient, ownerUsername, resp.GetProjectId(), commitResp.CommitId, diffRemoteToLocalResp)
	if err != nil {
		log.Panic(err)
	}

	err = statefile.StateFile{
		OwnerUsername: ownerUsername,
		ProjectId:     resp.ProjectId,
		CommitInfo: &statefile.CommitInfo{
			CommitId: commitResp.CommitId,
		},
	}.Save()
	if err != nil {
		panic(err)
	}
}

func InitConfig() {
	_, err := statefile.Find()
	if err == nil {
		fmt.Println("There's already a project initialized file here. Remove the `.jam` file to reinitialize.")
		os.Exit(1)
	}

	authFile, err := authfile.Authorize()
	if err != nil {
		fmt.Println("`~/.jamhubauth` file could not be found. Run `jam login` to create this file.")
		os.Exit(1)
	}

	apiClient, closer, err := jamgrpc.Connect(&oauth2.Token{
		AccessToken: string(authFile.Token),
	})
	if err != nil {
		log.Panic(err)
	}
	defer closer()

	for {
		fmt.Print("Create a new project (y/n)? ")
		var flag string
		fmt.Scanln(&flag)
		if strings.ToLower(flag) == "y" {
			fmt.Print("Project Name: ")
			var projectName string
			fmt.Scan(&projectName)
			InitNewProject(apiClient, projectName)
			break
		} else if strings.ToLower(flag) == "n" {
			fmt.Print("Project Name (`<owner>/<project name>`): ")
			var ownerProjectName string
			fmt.Scan(&ownerProjectName)
			s := strings.Split(ownerProjectName, "/")
			InitExistingProject(apiClient, s[0], s[1])
			break
		}
	}
}
