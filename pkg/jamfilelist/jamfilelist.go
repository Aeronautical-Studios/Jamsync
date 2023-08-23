package jamfilelist

import (
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"github.com/zdgeier/jam/gen/jampb"
	"google.golang.org/protobuf/proto"
)

type PathFileList []*jampb.PathFile

func (s PathFileList) Len() int {
	return len(s)
}
func (s PathFileList) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s PathFileList) Less(i, j int) bool {
	return s[i].Path < s[j].Path
}

func (s PathFileList) Find(path string) *jampb.PathFile {
	if index, ok := sort.Find(len(s), func(i int) int {
		return strings.Compare(path, s[i].Path)
	}); ok {
		return s[index]
	}
	return nil
}

func Diff(have *jampb.FileMetadata, want *jampb.FileMetadata) *jampb.FileMetadataDiff {
	haveFileList := PathFileList(have.Files)
	wantFileList := PathFileList(want.Files)

	fileMetadataDiff := make(map[string]*jampb.FileMetadataDiff_FileDiff, len(haveFileList))
	for _, pathFile := range haveFileList {
		fileMetadataDiff[pathFile.Path] = &jampb.FileMetadataDiff_FileDiff{
			Type: jampb.FileMetadataDiff_Delete,
		}
	}

	for _, wantFile := range wantFileList {
		var diffFile *jampb.File
		diffType := jampb.FileMetadataDiff_Delete
		haveFile := haveFileList.Find(wantFile.Path)
		if haveFile != nil && proto.Equal(wantFile.File, haveFile.File) {
			diffType = jampb.FileMetadataDiff_NoOp
		} else if haveFile != nil {
			fmt.Println("wjasdkl", hex.EncodeToString([]byte(wantFile.Path)), wantFile, haveFile)
			diffFile = wantFile.File
			diffType = jampb.FileMetadataDiff_Update
		} else {
			diffFile = wantFile.File
			diffType = jampb.FileMetadataDiff_Create
		}

		fileMetadataDiff[wantFile.Path] = &jampb.FileMetadataDiff_FileDiff{
			Type: diffType,
			File: diffFile,
		}
	}

	return &jampb.FileMetadataDiff{
		Diffs: fileMetadataDiff,
	}
}
