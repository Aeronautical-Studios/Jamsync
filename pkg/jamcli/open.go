package jamcli

import (
	"context"

	"github.com/pkg/browser"
	"github.com/zdgeier/jam/gen/jampb"
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
		_, err := statefile.Find()
		if err != nil {
			err = browser.OpenURL(url)
			if err != nil {
				panic(err)
			}
		}
	} else if jamenv.Env() == jamenv.Staging {
		url = "https://staging.jamhub.dev/"
		_, err := statefile.Find()
		if err != nil {
			err = browser.OpenURL(url)
			if err != nil {
				panic(err)
			}
		}
	} else if jamsite.Site() == jamsite.USWest2 {
		url = "https://west.jamhub.dev/"
		_, err := statefile.Find()
		if err != nil {
			err = browser.OpenURL(url)
			if err != nil {
				panic(err)
			}
		}
	}

	authFile, err := authfile.Authorize()
	if err != nil {
		panic(err)
	}

	conn, closer, err := jamgrpc.Connect(&oauth2.Token{
		AccessToken: string(authFile.Token),
	})
	if err != nil {
		panic(err)
	}
	defer closer()

	username := authFile.Username

	state, err := statefile.Find()
	if err != nil {
		err = browser.OpenURL(url + username + "/projects")
		if err != nil {
			panic(err)
		}
		return
	}

	apiClient := jampb.NewJamHubClient(conn)
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
		err = browser.OpenURL(url + username + "/" + nameResp.ProjectName + "/browse/?workspaceName=" + workspaceNameResp.GetWorkspaceName())
		if err != nil {
			panic(err)
		}
	} else {
		err = browser.OpenURL(url + username + "/" + nameResp.ProjectName + "/browse/")
		if err != nil {
			panic(err)
		}
	}
}
