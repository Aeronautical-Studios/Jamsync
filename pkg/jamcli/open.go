package jamcli

import (
	"context"

	"github.com/pkg/browser"
	"github.com/zdgeier/jam/gen/pb"
	"github.com/zdgeier/jam/pkg/jamcli/authfile"
	"github.com/zdgeier/jam/pkg/jamcli/statefile"
	"github.com/zdgeier/jam/pkg/jamenv"
	"github.com/zdgeier/jam/pkg/jamgrpc"
	"github.com/zdgeier/jam/pkg/jamsite"
	"golang.org/x/oauth2"
)

func Open() {
	url := "https://jamhub.dev/"
	if jamenv.Env() == jamenv.Local {
		url = "http://localhost/"
	} else if jamenv.Env() == jamenv.Staging {
		url = "https://staging.jamhub.dev/"
	} else if jamsite.Site() == jamsite.USWest2 {
		url = "https://west.jamhub.dev/"
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

	username := authFile.Username

	state, err := statefile.Find()
	if err != nil {
		err = browser.OpenURL(url + username + "/")
		if err != nil {
			panic(err)
		}
	}

	nameResp, err := apiClient.GetProjectName(context.Background(), &pb.GetProjectNameRequest{
		OwnerUsername: state.OwnerUsername,
		ProjectId:     state.ProjectId,
	})
	if err != nil {
		panic(err)
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
