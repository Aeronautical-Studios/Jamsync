package jamcli

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/zdgeier/jam/gen/jampb"
	"github.com/zdgeier/jam/pkg/jamcli/authfile"
	"github.com/zdgeier/jam/pkg/jamcli/statefile"
	"github.com/zdgeier/jam/pkg/jamgrpc"
	"golang.org/x/oauth2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func InitNewProject(conn *grpc.ClientConn, projectName string) {
	ctx := context.Background()
	apiClient := jampb.NewJamHubClient(conn)
	resp, err := apiClient.AddProject(ctx, &jampb.AddProjectRequest{
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

	workspaceResp, err := apiClient.CreateWorkspace(ctx, &jampb.CreateWorkspaceRequest{OwnerUsername: resp.GetOwnerUsername(), ProjectId: resp.ProjectId, WorkspaceName: "init"})
	if err != nil {
		fmt.Println("Project already exists")
		return
	}

	fileMetadata := ReadLocalFileList()
	fileMetadataDiff, err := diffLocalToRemoteCommit(apiClient, resp.GetOwnerUsername(), resp.GetProjectId(), workspaceResp.GetWorkspaceId(), fileMetadata)
	if err != nil {
		panic(err)
	}

	err = pushFileListDiffWorkspace(conn, resp.GetOwnerUsername(), resp.GetProjectId(), workspaceResp.WorkspaceId, 0, fileMetadata, fileMetadataDiff)
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

	// _, err = apiClient.DeleteWorkspace(context.Background(), &jampb.DeleteWorkspaceRequest{
	// 	OwnerUsername: resp.GetOwnerUsername(),
	// 	ProjectId:     resp.GetProjectId(),
	// 	WorkspaceId:   workspaceResp.WorkspaceId,
	// })
	// if err != nil {
	// 	log.Panic(err)
	// }

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

func IsEmpty(name string) (bool, error) {
	f, err := os.Open(name)
	if err != nil {
		return false, err
	}
	defer f.Close()

	_, err = f.Readdirnames(1)
	if err == io.EOF {
		return true, nil
	}
	return false, err
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

	diffRemoteToLocalResp, err := diffRemoteToLocalCommit(apiClient, ownerUsername, resp.ProjectId, commitResp.CommitId, &jampb.FileMetadata{})
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

	conn, closer, err := jamgrpc.Connect(&oauth2.Token{
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
			InitNewProject(conn, projectName)
			break
		} else if strings.ToLower(flag) == "n" {
			empty, err := IsEmpty(".")
			if err != nil {
				panic(err)
			}
			if !empty {
				fmt.Println("This directory is not empty. Please create a new directory and run `jam init` there.")
				os.Exit(1)
			}
			fmt.Print("Project Name (`<owner>/<project name>`): ")
			var ownerProjectName string
			fmt.Scan(&ownerProjectName)
			s := strings.Split(ownerProjectName, "/")
			InitExistingProject(jampb.NewJamHubClient(conn), s[0], s[1])
			break
		}
	}
}
