package jamignore

import (
	"strings"

	"github.com/zdgeier/jam/pkg/gitignore"
)

type JamHubIgnorer struct {
	gitignorer *gitignore.GitIgnore
	jamignorer *gitignore.GitIgnore
}

func (j *JamHubIgnorer) ImportPatterns() error {
	jamignorer, _ := gitignore.CompileIgnoreFile(".jamignore")
	j.jamignorer = jamignorer

	gitignorer, _ := gitignore.CompileIgnoreFile(".gitignore")
	j.gitignorer = gitignorer

	return nil
}

func (j *JamHubIgnorer) Match(filepath string) bool {
	if strings.HasPrefix(filepath, ".git") || strings.HasPrefix(filepath, ".jam") {
		return true
	}

	if j.gitignorer != nil && j.gitignorer.MatchesPath(filepath) {
		return true
	}

	if j.jamignorer != nil && j.jamignorer.MatchesPath(filepath) {
		return true
	}

	return false
}
