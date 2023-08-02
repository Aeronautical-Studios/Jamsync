package jamcli

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/zdgeier/jam/gen/jampb"
	"github.com/zdgeier/jam/pkg/jamcli/authfile"
	"github.com/zdgeier/jam/pkg/jamgrpc"
	"golang.org/x/oauth2"
)

func ListProjects() {
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
		log.Panic(err)
	}

	resp, err := apiClient.ListUserProjects(ctx, &jampb.ListUserProjectsRequest{})
	if err != nil {
		log.Panic(err)
	}

	for _, proj := range resp.GetProjects() {
		fmt.Println(proj.GetOwnerUsername() + "/" + proj.GetName())
	}
}
