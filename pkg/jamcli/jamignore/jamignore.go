package jamignore

import (
	"os"
	"strings"

	"github.com/zdgeier/jam/pkg/gitignore"
)

type JamHubIgnorer struct {
	gitignorer *gitignore.GitIgnore
}

func (j *JamHubIgnorer) ImportPatterns(filepath string) error {
	file, err := os.ReadFile(filepath)
	if err != nil {
		return nil
	}

	patterns := strings.Split(string(file), "\n")

	gitignorer, err := gitignore.CompileIgnoreLines(patterns...)
	if err != nil {
		return err
	}
	j.gitignorer = gitignorer

	return nil
}

func (j *JamHubIgnorer) Match(filepath string) bool {
	if strings.HasPrefix(filepath, ".git") || strings.HasPrefix(filepath, ".jam") {
		return true
	}

	return j.gitignorer.MatchesPath(filepath)
}
