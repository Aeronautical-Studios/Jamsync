package jamignore

import (
	"fmt"
	"os"
	"strings"

	"github.com/zdgeier/jam/pkg/gitignore"
)

//	type JamHubIgnorer struct {
//		globs []glob.Glob
//	}
//
//	func (j *JamHubIgnorer) ImportPatterns(filepath string) error {
//		file, err := os.ReadFile(filepath)
//		if err != nil {
//			return nil
//		}
//
//		patterns := strings.Split(string(file), "\n")
//		newGlobs := make([]glob.Glob, 0, len(patterns))
//
//		for _, line := range patterns {
//			if strings.HasPrefix(line, "#") || len(strings.TrimSpace(line)) == 0 {
//				continue
//			}
//
//			j.globs = append(j.globs, glob.MustCompile(strings.TrimSpace(line)))
//			// // TODO: Hacking this together for now. This should be made properly.
//			// line = strings.ReplaceAll(line, "/", "*")
//
//			pat, _, _ := strings.Cut(line, "#")
//
//			lineGlob, err := glob.Compile(strings.TrimSpace(pat))
//			if err != nil {
//				return err
//			}
//			j.globs = append(j.globs, lineGlob)
//		}
//		fmt.Println(patterns)
//
//		j.globs = append(j.globs, newGlobs...)
//
//		return nil
//	}
//
//	func (j *JamHubIgnorer) Match(filepath string) bool {
//		for _, g := range j.globs {
//			if g.Match(filepath) {
//				fmt.Println("Matched", filepath)
//				return true
//			}
//		}
//		fmt.Println("nomatch", filepath)
//		return false
//	}
type JamHubIgnorer struct {
	globs []string
}

func (j *JamHubIgnorer) ImportPatterns(filepath string) error {
	file, err := os.ReadFile(filepath)
	if err != nil {
		return nil
	}

	patterns := strings.Split(string(file), "\n")

	for _, line := range patterns {
		if strings.HasPrefix(line, "#") || len(strings.TrimSpace(line)) == 0 {
			continue
		}

		j.globs = append(j.globs, strings.TrimSpace(line))
		// // TODO: Hacking this together for now. This should be made properly.
		// line = strings.ReplaceAll(line, "/", "*")

		// pat, _, _ := strings.Cut(line, "#")

		// lineGlob, err := strings.TrimSpace(pat)
		// if err != nil {
		// 	return err
		// }
		// j.globs = append(j.globs, lineGlob)
	}

	fmt.Println(j.globs)

	return nil
}

func (j *JamHubIgnorer) Match(filepath string) bool {
	if strings.HasPrefix(filepath, ".git/") || strings.HasPrefix(filepath, ".jam") {
		return true
	}

	for _, g := range j.globs {
		// if strings.HasPrefix(g, "node_modules") {
		// 	fmt.Println("test", g, filepath, gitignore.Match(g, filepath))
		// }
		if gitignore.Match(g, filepath) {
			fmt.Println("Matched", filepath)
			return true
		}
	}
	// fmt.Println("nomatch", filepath)
	return false
}
