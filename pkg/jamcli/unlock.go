package jamcli

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"
	"errors"
	"path/filepath"

	"github.com/zdgeier/jam/gen/jampb"
	"github.com/zdgeier/jam/pkg/jamcli/authfile"
	"github.com/zdgeier/jam/pkg/jamcli/statefile"
	"github.com/zdgeier/jam/pkg/jamgrpc"
	"golang.org/x/oauth2"
	b64 "encoding/base64"
)

// Lock locks a file or directory in the current project.
func UnLock() {
	if len(os.Args) != 3 {
		fmt.Println("jam unlock <relative file path>")
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

	_, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err != nil {
		panic(err)
	}

	state, err := statefile.Find()
	if err != nil {
		fmt.Println("Could not find a `.jam` file. Run `jam init` to initialize the project.")
		os.Exit(0)
	}

	resp, err := apiClient.CurrentUser(context.Background(), &jampb.CurrentUserRequest{})
	if err != nil {
		panic(err)
	}

	cleanpath, err := unlockFile(apiClient, resp.GetUsername(), state.ProjectId, os.Args[2])

	if err != nil {
		log.Panic(err)
	}

	fmt.Println("Removed Lock on", cleanpath)
}

// private methods

// unlock the file given by path
func unlockFile(apiClient jampb.JamHubClient, username string, projectId uint64, path string) (string, error) {
	path = filepath.Clean(path)

	absFilePath, err := getAbsPath(path)
	if err != nil {
		return "", errors.New(fmt.Sprintf("could not resolve file path: %s", err.Error()))
	}

	fileInfo, err := os.Stat(absFilePath)
	if err != nil {
		panic(err)
	}

	res, err := apiClient.UpdateFileLock(context.Background(), &jampb.UpdateFileLockRequest{
		ProjectId: projectId,
		OwnerUsername: username,
		B64EncodedPath: b64.URLEncoding.EncodeToString([]byte(path)),
		IsDir: fileInfo.IsDir(),
		LockUnlockFlag: false,
	})

	if err != nil {
		return "", err
	}

	if res.GetIsLocked() {
		return "", errors.New("could not unlock file")
	}

	return path, nil
}