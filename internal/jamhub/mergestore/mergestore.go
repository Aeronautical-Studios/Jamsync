package mergestore

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
)

type LocalStore struct {
}

// Used to merge files. Temporarily stores them locally to use diff3 utility.
func NewLocalMergeStore() *LocalStore {
	return &LocalStore{}
}

func (s *LocalStore) filePath(ownerUsername string, projectId uint64, pathHash []byte) string {
	return fmt.Sprintf("/tmp/jamhubmergestore/%s/%d/%02X", ownerUsername, projectId, pathHash)
}

func (s *LocalStore) fileDir(ownerUsername string, projectId uint64) string {
	return fmt.Sprintf("/tmp/jamhubmergestore/%s/%d", ownerUsername, projectId)
}

func (s *LocalStore) Merge(ownerUsername string, projectId uint64, pathHash []byte, old, mine, theirs *bytes.Reader) (*bytes.Reader, error) {
	err := os.MkdirAll(s.fileDir(ownerUsername, projectId), os.ModePerm)
	if err != nil {
		panic(err)
	}

	filePath := s.filePath(ownerUsername, projectId, pathHash)

	oldBytes, err := io.ReadAll(old)
	if err != nil {
		panic(err)
	}
	err = os.WriteFile(string(filePath)+".old", oldBytes, os.ModePerm)
	if err != nil {
		panic(err)
	}

	mineBytes, err := io.ReadAll(old)
	if err != nil {
		panic(err)
	}
	err = os.WriteFile(string(filePath)+".mine", mineBytes, os.ModePerm)
	if err != nil {
		panic(err)
	}

	theirBytes, err := io.ReadAll(theirs)
	if err != nil {
		panic(err)
	}
	err = os.WriteFile(string(filePath)+".theirs", theirBytes, os.ModePerm)
	if err != nil {
		panic(err)
	}

	out, _ := exec.Command("diff3", "-m", string(filePath)+".old", string(filePath)+".mine", string(filePath)+".theirs").Output()

	os.Remove(string(filePath) + ".old")
	os.Remove(string(filePath) + ".mine")
	os.Remove(string(filePath) + ".theirs")

	return bytes.NewReader(out), nil
}
