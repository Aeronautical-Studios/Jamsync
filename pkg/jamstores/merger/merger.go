package merger

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/zdgeier/jam/pkg/jamstores/file"
)

func Merge(ownerUsername string, projectId uint64, metadataFilePath string, old, mine, theirs *bytes.Buffer) (*bytes.Reader, error) {
	err := os.MkdirAll(fmt.Sprintf("/tmp/jamhubmergestore/%s/%d", ownerUsername, projectId), os.ModePerm)
	if err != nil {
		panic(err)
	}

	filePath := fmt.Sprintf("/tmp/jamhubmergestore/%s/%d/%02X", ownerUsername, projectId, file.PathToHash(metadataFilePath))

	oldBytes, err := io.ReadAll(old)
	if err != nil {
		panic(err)
	}
	oldPath := string(filePath) + ".old"
	err = os.WriteFile(oldPath, oldBytes, os.ModePerm)
	if err != nil {
		panic(err)
	}

	mineBytes, err := io.ReadAll(mine)
	if err != nil {
		panic(err)
	}
	minePath := string(filePath) + ".mine"
	err = os.WriteFile(minePath, mineBytes, os.ModePerm)
	if err != nil {
		panic(err)
	}

	theirBytes, err := io.ReadAll(theirs)
	if err != nil {
		panic(err)
	}
	theirsPath := string(filePath) + ".theirs"
	err = os.WriteFile(theirsPath, theirBytes, os.ModePerm)
	if err != nil {
		panic(err)
	}

	out, _ := exec.Command("diff3", "-m", minePath, oldPath, theirsPath).Output()

	os.Remove(oldPath)
	os.Remove(minePath)
	os.Remove(theirsPath)

	oldString := strings.ReplaceAll(string(out), oldPath, metadataFilePath+".old")
	mineString := strings.ReplaceAll(string(oldString), minePath, metadataFilePath+".mine")
	theirsString := strings.ReplaceAll(string(mineString), theirsPath, metadataFilePath+".theirs")

	return bytes.NewReader([]byte(theirsString)), nil
}
