package authfile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path"

	"github.com/zdgeier/jam/gen/jampb"
	"github.com/zdgeier/jam/pkg/jamenv"
	"github.com/zdgeier/jam/pkg/jamgrpc"
	"github.com/zdgeier/jam/pkg/jamstores/clientauth"
	"golang.org/x/oauth2"
)

type AuthFile struct {
	Username string `json:"username"`
	Token    string `json:"token"`
}

func Authorize() (AuthFile, error) {
	rawFile, err := os.ReadFile(authPath())
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return AuthFile{}, err
	}

	if errors.Is(err, os.ErrNotExist) {
		token := ""
		if os.Args[1] == "login" && len(os.Args) > 2 && os.Args[2] != "" && jamenv.Env() == jamenv.Local {
			token = os.Args[2]
		} else if jamenv.Env() == jamenv.Local {
			if len(os.Args) < 3 {
				fmt.Println("Use `JAM_ENV=local jam login <username>`")
				os.Exit(1)
			} else {
				fmt.Println("No ~/.jamhubauth file. Login with `JAM_ENV=local jam login <username>` first")
				os.Exit(1)
			}
		} else {
			token, err = clientauth.AuthorizeUser()
			if err != nil {
				log.Panic(err)
			}
		}

		apiClient, closer, err := jamgrpc.Connect(&oauth2.Token{
			AccessToken: string(token),
		})
		if err != nil {
			log.Panic(err)
		}
		defer closer()

		ctx := context.Background()

		resp, err := apiClient.CurrentUser(ctx, &jampb.CurrentUserRequest{})
		if err != nil {
			fmt.Println("Log into the website first to create your account.")
		}

		authFile := AuthFile{
			Token:    token,
			Username: resp.GetUsername(),
		}

		data, err := json.Marshal(authFile)
		if err != nil {
			log.Panic(err)
		}

		err = os.WriteFile(authPath(), data, fs.ModePerm)
		if err != nil {
			log.Panic(err)
		}
		return authFile, nil
	}

	authFile := AuthFile{}
	err = json.Unmarshal(rawFile, &authFile)
	if err != nil {
		return authFile, err
	}

	apiClient, closer, err := jamgrpc.Connect(&oauth2.Token{
		AccessToken: string(authFile.Token),
	})
	if err != nil {
		log.Panic(err)
	}
	defer closer()

	_, err = apiClient.CurrentUser(context.Background(), &jampb.CurrentUserRequest{})
	if err != nil {
		// If outdated token
		err := os.Remove(authPath())
		if err != nil {
			return authFile, err
		}
		Authorize()
	}

	return authFile, nil
}

func Logout() error {
	return os.Remove(authPath())
}

func authPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return path.Join(home, ".jamhubauth")
}
