package jam

import (
	"context"
	"fmt"

	"github.com/pkg/browser"
	"github.com/zdgeier/jamhub/gen/pb"
	"github.com/zdgeier/jamhub/internal/jam/authfile"
	"github.com/zdgeier/jamhub/internal/jam/statefile"
	"github.com/zdgeier/jamhub/internal/jamenv"
	"github.com/zdgeier/jamhub/internal/jamhubgrpc"
	"golang.org/x/oauth2"
)

func Open() {
	state, err := statefile.Find()
	if err != nil {
		fmt.Println("Could not find a `.jam` file. Run `jam init` to initialize the project.")
		return
	}

	authFile, err := authfile.Authorize()
	if err != nil {
		panic(err)
	}

	apiClient, closer, err := jamhubgrpc.Connect(&oauth2.Token{
		AccessToken: string(authFile.Token),
	})
	if err != nil {
		panic(err)
	}
	defer closer()

	nameResp, err := apiClient.GetProjectName(context.Background(), &pb.GetProjectNameRequest{
		OwnerUsername: state.OwnerUsername,
		ProjectId:     state.ProjectId,
	})
	if err != nil {
		panic(err)
	}

	url := "https://jamhub.dev/"
	username := authFile.Username
	if jamenv.Env() == jamenv.Local {
		url = "http://localhost/"
	} else if jamenv.Env() == jamenv.Staging {
		url = "https://staging.jamhub.dev/"
	}

	if state.WorkspaceInfo != nil {
		workspaceNameResp, err := apiClient.GetWorkspaceName(context.Background(), &pb.GetWorkspaceNameRequest{
			ProjectId:     state.ProjectId,
			OwnerUsername: state.OwnerUsername,
			WorkspaceId:   state.WorkspaceInfo.WorkspaceId,
		})
		if err != nil {
			panic(err)
		}
		err = browser.OpenURL(url + username + "/" + nameResp.ProjectName + "/workspacefiles/" + workspaceNameResp.GetWorkspaceName() + "/")
		if err != nil {
			panic(err)
		}
	} else {
		err = browser.OpenURL(url + username + "/" + nameResp.ProjectName + "/committedfiles/")
		if err != nil {
			panic(err)
		}
	}
}
