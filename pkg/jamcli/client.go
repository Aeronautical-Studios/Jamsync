package jamcli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/schollz/progressbar/v3"
	"github.com/zdgeier/jam/gen/jampb"
	"github.com/zdgeier/jam/pkg/fastcdc"
	"github.com/zdgeier/jam/pkg/jamcli/jamignore"
	"github.com/zdgeier/jam/pkg/jamstores/file"
	"github.com/zeebo/xxh3"
	"google.golang.org/protobuf/proto"
)

func DiffHasChanges(diff *jampb.FileMetadataDiff) bool {
	for _, diff := range diff.GetDiffs() {
		if diff.Type != jampb.FileMetadataDiff_NoOp {
			return true
		}
	}
	return false
}

type PathFile struct {
	path string
	file *jampb.File
}

type PathInfo struct {
	path  string
	isDir bool
}

func worker(pathInfos <-chan PathInfo, results chan<- PathFile) {
	for pathInfo := range pathInfos {
		osFile, err := os.Open(pathInfo.path)
		if err != nil {
			fmt.Println("Could not open ", pathInfo.path, ":", err)
			results <- PathFile{}
			continue
		}

		var file *jampb.File
		if pathInfo.isDir {
			file = &jampb.File{
				Dir: true,
			}
		} else {
			data, err := os.ReadFile(pathInfo.path)
			if err != nil {
				fmt.Println("Could not read ", pathInfo.path, "(jam does not support symlinks yet)")
				results <- PathFile{}
				continue
			}
			b := xxh3.Hash128(data).Bytes()

			file = &jampb.File{
				Dir:  false,
				Hash: b[:],
			}
		}
		osFile.Close()
		results <- PathFile{pathInfo.path, file}
	}
}

func ReadLocalFileList() *jampb.FileMetadata {
	var ignorer = &jamignore.JamHubIgnorer{}
	err := ignorer.ImportPatterns()
	if err != nil {
		panic(err)
	}
	var numEntries int64
	i := 0
	if err := filepath.WalkDir(".", func(path string, d fs.DirEntry, _ error) error {
		if path == "." {
			return nil
		}
		path = filepath.ToSlash(path)
		if ignorer.Match(path) {
			return nil
		}
		numEntries += 1
		i += 1
		return nil
	}); err != nil {
		fmt.Println("WARN: could not walk directory tree", err)
	}

	paths := make(chan PathInfo, numEntries)
	results := make(chan PathFile, numEntries)

	i = 0
	for w := 1; w < 2048 && w <= int(numEntries)/10+1; w++ {
		go worker(paths, results)
	}

	go func() {
		if err := filepath.WalkDir(".", func(path string, d fs.DirEntry, _ error) error {
			if path == "." {
				return nil
			}
			path = filepath.ToSlash(path)
			if ignorer.Match(path) {
				return nil
			}
			paths <- PathInfo{path, d.IsDir()}
			i += 1
			return nil
		}); err != nil {
			fmt.Println("WARN: could not walk directory tree", err)
		}
		close(paths)
	}()

	files := make(map[string]*jampb.File, numEntries)
	for i := int64(0); i < numEntries; i++ {
		pathFile := <-results
		if pathFile.path != "" {
			files[pathFile.path] = pathFile.file
		}
	}

	return &jampb.FileMetadata{
		Files: files,
	}
}

func uploadWorkspaceFile(apiClient jampb.JamHubClient, ownerUsername string, projectId uint64, workspaceId uint64, changeId uint64, filePath string, sourceReader io.Reader, token []byte, writeStream jampb.JamHub_WriteWorkspaceOperationsStreamClient) error {
	ctx := context.Background()

	client, err := apiClient.ReadWorkspaceFileHashes(ctx, &jampb.ReadWorkspaceFileHashesRequest{
		OwnerUsername: ownerUsername,
		ProjectId:     projectId,
		WorkspaceId:   workspaceId,
		ChangeId:      changeId,
		PathHashes:    [][]byte{pathToHash(filePath)},
	})
	if err != nil {
		return err
	}

	sourceChunker, err := fastcdc.NewJamChunker(sourceReader)
	if err != nil {
		return err
	}

	msg, err := client.Recv()
	if err != nil {
		return err
	}

	err = sourceChunker.CreateDelta(msg.Hashes, func(chunk *jampb.Chunk) error {
		err = writeStream.Send(&jampb.FileWriteOperation{
			PathHash:       pathToHash(filePath),
			Chunk:          chunk,
			OperationToken: token,
		})
		if err != nil {
			log.Panic(err)
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	return err
}

func pathToHash(path string) []byte {
	h := xxh3.Hash128([]byte(path)).Bytes()
	return h[:]
}

func pushFileListDiffWorkspace(apiClient jampb.JamHubClient, ownerUsername string, projectId uint64, workspaceId uint64, currChangeId uint64, fileMetadata *jampb.FileMetadata, fileMetadataDiff *jampb.FileMetadataDiff) error {
	ctx := context.Background()

	numFiles := 0
	totalSize := int64(0)
	for path, diff := range fileMetadataDiff.GetDiffs() {
		if diff.GetType() != jampb.FileMetadataDiff_NoOp && diff.GetType() != jampb.FileMetadataDiff_Delete && !diff.GetFile().GetDir() {
			numFiles += 1
			s, err := os.Stat(path)
			if err != nil {
				panic(err)
			}
			totalSize += s.Size()
		}
	}

	tokenResp, err := apiClient.GetOperationStreamToken(ctx, &jampb.GetOperationStreamTokenRequest{
		OwnerUsername: ownerUsername,
		ProjectId:     projectId,
		WorkspaceId:   workspaceId,
	})
	if err != nil {
		panic(err)
	}

	type jobData struct {
		pathHash []byte
		hashes   map[uint64][]byte
	}

	pathHashToPath := make(map[string]string)

	bar := progressbar.DefaultBytes(int64(totalSize), "Uploading")
	var wg sync.WaitGroup
	worker := func(jobs <-chan jobData, results chan<- error) {
		defer wg.Done()

		writeStream, err := apiClient.WriteWorkspaceOperationsStream(ctx)
		if err != nil {
			panic(err)
		}
		for job := range jobs {
			path := pathHashToPath[string(job.pathHash)]
			file, err := os.OpenFile(path, os.O_RDONLY, 0755)
			if err != nil {
				panic(err)
			}

			sourceChunker, err := fastcdc.NewJamChunker(file)
			if err != nil {
				panic(err)
			}

			err = sourceChunker.CreateDelta(job.hashes, func(chunk *jampb.Chunk) error {
				err := writeStream.Send(&jampb.FileWriteOperation{
					PathHash:       pathToHash(path),
					Chunk:          chunk,
					OperationToken: tokenResp.GetToken(),
				})
				bar.Write(chunk.Data)
				return err
			})
			if err != nil {
				fmt.Println(err)
				panic(err)
			}

			err = file.Close()
			if err != nil {
				panic(err)
			}

			results <- nil
		}
		_, err = writeStream.CloseAndRecv()
		if err != nil {
			panic(err)
		}
	}

	jobs := make(chan jobData, numFiles)
	results := make(chan error, numFiles)
	for w := 1; w < 64 && w <= int(numFiles)/10+1; w++ {
		wg.Add(1)
		go worker(jobs, results)
	}

	pathHashes := make([][]byte, 0, numFiles)
	for path, diff := range fileMetadataDiff.GetDiffs() {
		if diff.GetType() != jampb.FileMetadataDiff_NoOp && diff.GetType() != jampb.FileMetadataDiff_Delete && !diff.GetFile().GetDir() {
			pathHashes = append(pathHashes, pathToHash(path))
			pathHashToPath[string(pathToHash(path))] = path
		}
	}

	client, err := apiClient.ReadWorkspaceFileHashes(ctx, &jampb.ReadWorkspaceFileHashesRequest{
		OwnerUsername: ownerUsername,
		ProjectId:     projectId,
		WorkspaceId:   workspaceId,
		ChangeId:      currChangeId,
		PathHashes:    pathHashes,
	})
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			recv, err := client.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {

				panic(err)
			}
			jobs <- jobData{
				pathHash: recv.PathHash,
				hashes:   recv.Hashes,
			}
		}
		close(jobs)
	}()

	for a := 1; a <= numFiles; a++ {
		err := <-results
		if err != nil {
			return err
		}
	}
	bar.Finish()

	metadataBytes, err := proto.Marshal(fileMetadata)
	if err != nil {
		return err
	}
	writeStream, err := apiClient.WriteWorkspaceOperationsStream(ctx)
	if err != nil {
		panic(err)
	}
	err = uploadWorkspaceFile(apiClient, ownerUsername, projectId, workspaceId, tokenResp.NewChangeId, ".jamfilelist", bytes.NewReader(metadataBytes), tokenResp.GetToken(), writeStream)
	if err != nil {
		return err
	}
	_, err = writeStream.CloseAndRecv()
	if err != nil {
		return err
	}

	fmt.Println("Uploading file list...")
	wg.Wait()

	return nil
}

func ApplyFileListDiffCommit(apiClient jampb.JamHubClient, ownerUsername string, projectId, commitId uint64, fileMetadataDiff *jampb.FileMetadataDiff) error {
	ctx := context.Background()
	for path, diff := range fileMetadataDiff.GetDiffs() {
		if diff.GetType() != jampb.FileMetadataDiff_NoOp && diff.GetFile().GetDir() {
			err := os.MkdirAll(path, os.ModePerm)
			if err != nil {
				return err
			}
		}
	}
	var numFiles int64
	for _, diff := range fileMetadataDiff.GetDiffs() {
		if diff.GetType() != jampb.FileMetadataDiff_NoOp && !diff.GetFile().GetDir() {
			numFiles += 1
		}
	}

	if numFiles == 0 {
		return nil
	}

	worker := func(jobs <-chan string, results chan<- error) {
		for path := range jobs {
			currFile, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE, 0755)
			if err != nil {
				panic(err)
			}

			targetChunker, err := fastcdc.NewJamChunker(currFile)
			if err != nil {
				panic(err)
			}

			localChunkHashes, err := targetChunker.CreateHashSignature()
			if err != nil {
				panic(err)
			}

			readFileClient, err := apiClient.ReadCommittedFile(ctx, &jampb.ReadCommittedFileRequest{
				ProjectId:        projectId,
				OwnerUsername:    ownerUsername,
				CommitId:         commitId,
				PathHash:         pathToHash(path),
				LocalChunkHashes: localChunkHashes,
			})
			if err != nil {
				panic(err)
			}
			tempFilePath := path + ".jamtemp"
			tempFile, err := os.OpenFile(tempFilePath, os.O_WRONLY|os.O_CREATE, 0755)
			if err != nil {
				panic(err)
			}
			ops := make(chan *jampb.Chunk)
			go func() {
				for {
					in, err := readFileClient.Recv()
					if err == io.EOF {
						break
					}
					if err != nil {
						log.Println(err)
						return
					}
					ops <- in.Chunk
				}
				close(ops)
			}()

			currFile.Seek(0, 0)
			err = targetChunker.ApplyDelta(tempFile, currFile, ops)
			if err != nil {
				panic(err)
			}
			err = currFile.Close()
			if err != nil {
				panic(err)
			}

			err = tempFile.Close()
			if err != nil {
				panic(err)
			}

			err = os.Rename(tempFilePath, path)
			if err != nil {
				panic(err)
			}
			results <- nil
		}
	}

	jobs := make(chan string, numFiles)
	results := make(chan error, numFiles)

	for w := 1; w <= 256; w++ {
		go worker(jobs, results)
	}

	bar := progressbar.Default(numFiles, "Downloading")
	go func() {
		for path, diff := range fileMetadataDiff.GetDiffs() {
			if diff.GetType() != jampb.FileMetadataDiff_NoOp && !diff.GetFile().GetDir() {
				if diff.GetType() == jampb.FileMetadataDiff_Delete {
					err := os.Remove(path)
					if err != nil {
						panic(err)
					}
					bar.ChangeMax(bar.GetMax() - 1)
				} else {
					jobs <- path
				}
			}
		}
	}()

	for a := int64(1); a <= numFiles; a++ {
		err := <-results
		if err != nil {
			return err
		}
		bar.Add(1)
	}
	bar.Finish()

	return nil
}

func ApplyFileListDiffWorkspace(apiClient jampb.JamHubClient, ownerUsername string, projectId uint64, workspaceId uint64, changeId uint64, fileMetadataDiff *jampb.FileMetadataDiff) error {
	for path, diff := range fileMetadataDiff.GetDiffs() {
		if diff.GetType() != jampb.FileMetadataDiff_NoOp && diff.GetFile().GetDir() {
			err := os.MkdirAll(path, os.ModePerm)
			if err != nil {
				return err
			}
		}
	}
	var numFiles int64
	for _, diff := range fileMetadataDiff.GetDiffs() {
		if diff.GetType() != jampb.FileMetadataDiff_NoOp && !diff.GetFile().GetDir() {
			numFiles += 1
		}
	}

	if numFiles == 0 {
		return nil
	}

	for path, diff := range fileMetadataDiff.GetDiffs() {
		if diff.GetType() != jampb.FileMetadataDiff_NoOp && !diff.GetFile().GetDir() {
			if diff.GetType() == jampb.FileMetadataDiff_Delete {
				err := os.Remove(path)
				if err != nil {
					fmt.Println(err)
				}
			} else {

				currFile, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE, 0755)
				if err != nil {
					return err
				}

				targetChunker, err := fastcdc.NewJamChunker(currFile)
				if err != nil {
					return err
				}

				localChunkHashes, err := targetChunker.CreateHashSignature()
				if err != nil {
					return err
				}

				readFileClient, err := apiClient.ReadWorkspaceFile(context.Background(), &jampb.ReadWorkspaceFileRequest{
					ProjectId:        projectId,
					OwnerUsername:    ownerUsername,
					WorkspaceId:      workspaceId,
					ChangeId:         changeId,
					PathHash:         pathToHash(path),
					LocalChunkHashes: localChunkHashes,
				})
				if err != nil {
					return err
				}
				ops := make(chan *jampb.Chunk)
				go func() {
					for {
						in, err := readFileClient.Recv()
						if err == io.EOF {
							break
						}
						if err != nil {
							log.Println(err)
							return
						}
						ops <- in.Chunk
					}
					close(ops)
				}()
				tempFilePath := path + ".jamtemp"
				tempFile, err := os.OpenFile(tempFilePath, os.O_WRONLY|os.O_CREATE, 0755)
				if err != nil {
					return err
				}

				currFile.Seek(0, 0)
				err = targetChunker.ApplyDelta(tempFile, currFile, ops)
				if err != nil {
					return err
				}
				err = currFile.Close()
				if err != nil {
					return err
				}

				err = tempFile.Close()
				if err != nil {
					return err
				}

				err = os.Rename(tempFilePath, path)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func DiffRemoteToLocalCommit(apiClient jampb.JamHubClient, ownerUsername string, projectId uint64, commitId uint64, localFileMetadata *jampb.FileMetadata) (*jampb.FileMetadataDiff, error) {
	metadataBytes, err := proto.Marshal(localFileMetadata)
	if err != nil {
		return nil, err
	}
	metadataReader := bytes.NewReader(metadataBytes)
	metadataResult := new(bytes.Buffer)
	err = file.DownloadCommittedFile(apiClient, ownerUsername, projectId, commitId, ".jamfilelist", metadataReader, metadataResult)
	if err != nil {
		return nil, err
	}

	remoteFileMetadata := &jampb.FileMetadata{}
	err = proto.Unmarshal(metadataResult.Bytes(), remoteFileMetadata)
	if err != nil {
		return nil, err
	}

	fileMetadataDiff := make(map[string]*jampb.FileMetadataDiff_FileDiff, len(localFileMetadata.GetFiles()))
	for filePath := range localFileMetadata.GetFiles() {
		fileMetadataDiff[filePath] = &jampb.FileMetadataDiff_FileDiff{
			Type: jampb.FileMetadataDiff_Delete,
		}
	}

	for filePath, file := range remoteFileMetadata.GetFiles() {
		var diffFile *jampb.File
		diffType := jampb.FileMetadataDiff_Delete
		remoteFile, found := localFileMetadata.GetFiles()[filePath]
		if found && proto.Equal(file, remoteFile) {
			diffType = jampb.FileMetadataDiff_NoOp
		} else if found {
			diffFile = file
			diffType = jampb.FileMetadataDiff_Update
		} else {
			diffFile = file
			diffType = jampb.FileMetadataDiff_Create
		}

		fileMetadataDiff[filePath] = &jampb.FileMetadataDiff_FileDiff{
			Type: diffType,
			File: diffFile,
		}
	}

	return &jampb.FileMetadataDiff{
		Diffs: fileMetadataDiff,
	}, err
}

func DiffRemoteToLocalWorkspace(apiClient jampb.JamHubClient, ownerUsername string, projectId uint64, workspaceId uint64, changeId uint64, fileMetadata *jampb.FileMetadata) (*jampb.FileMetadataDiff, error) {
	metadataBytes, err := proto.Marshal(fileMetadata)
	if err != nil {
		return nil, err
	}
	metadataReader := bytes.NewReader(metadataBytes)
	metadataResult := new(bytes.Buffer)
	err = file.DownloadWorkspaceFile(apiClient, ownerUsername, projectId, workspaceId, changeId, ".jamfilelist", metadataReader, metadataResult)
	if err != nil {
		return nil, err
	}

	remoteFileMetadata := &jampb.FileMetadata{}
	err = proto.Unmarshal(metadataResult.Bytes(), remoteFileMetadata)
	if err != nil {
		return nil, err
	}

	fileMetadataDiff := make(map[string]*jampb.FileMetadataDiff_FileDiff, len(fileMetadata.GetFiles()))
	for filePath := range fileMetadata.GetFiles() {
		fileMetadataDiff[filePath] = &jampb.FileMetadataDiff_FileDiff{
			Type: jampb.FileMetadataDiff_Delete,
		}
	}

	for filePath, file := range remoteFileMetadata.GetFiles() {
		var diffFile *jampb.File
		diffType := jampb.FileMetadataDiff_Delete
		remoteFile, found := fileMetadata.GetFiles()[filePath]
		if found && proto.Equal(file, remoteFile) {
			diffType = jampb.FileMetadataDiff_NoOp
		} else if found {
			diffFile = file
			diffType = jampb.FileMetadataDiff_Update
		} else {
			diffFile = file
			diffType = jampb.FileMetadataDiff_Create
		}

		fileMetadataDiff[filePath] = &jampb.FileMetadataDiff_FileDiff{
			Type: diffType,
			File: diffFile,
		}
	}

	return &jampb.FileMetadataDiff{
		Diffs: fileMetadataDiff,
	}, err
}

func diffLocalToRemoteCommit(apiClient jampb.JamHubClient, ownerUsername string, projectId uint64, commitId uint64, fileMetadata *jampb.FileMetadata) (*jampb.FileMetadataDiff, error) {
	metadataBytes, err := proto.Marshal(fileMetadata)
	if err != nil {
		return nil, err
	}
	metadataReader := bytes.NewReader(metadataBytes)
	metadataResult := new(bytes.Buffer)
	err = file.DownloadCommittedFile(apiClient, ownerUsername, projectId, commitId, ".jamfilelist", metadataReader, metadataResult)
	if err != nil {
		return nil, err
	}

	remoteFileMetadata := &jampb.FileMetadata{}
	err = proto.Unmarshal(metadataResult.Bytes(), remoteFileMetadata)
	if err != nil {
		return nil, err
	}

	fileMetadataDiff := make(map[string]*jampb.FileMetadataDiff_FileDiff, len(remoteFileMetadata.GetFiles()))
	for remoteFilePath := range remoteFileMetadata.GetFiles() {
		fileMetadataDiff[remoteFilePath] = &jampb.FileMetadataDiff_FileDiff{
			Type: jampb.FileMetadataDiff_Delete,
		}
	}

	for filePath, file := range fileMetadata.GetFiles() {
		var diffFile *jampb.File
		diffType := jampb.FileMetadataDiff_Delete
		remoteFile, found := remoteFileMetadata.GetFiles()[filePath]
		if found && proto.Equal(file, remoteFile) {
			diffType = jampb.FileMetadataDiff_NoOp
		} else if found {
			diffFile = file
			diffType = jampb.FileMetadataDiff_Update
		} else {
			diffFile = file
			diffType = jampb.FileMetadataDiff_Create
		}

		fileMetadataDiff[filePath] = &jampb.FileMetadataDiff_FileDiff{
			Type: diffType,
			File: diffFile,
		}
	}

	return &jampb.FileMetadataDiff{
		Diffs: fileMetadataDiff,
	}, err
}

func DiffLocalToRemoteWorkspace(apiClient jampb.JamHubClient, ownerId string, projectId uint64, workspaceId uint64, changeId uint64, fileMetadata *jampb.FileMetadata) (*jampb.FileMetadataDiff, error) {
	metadataBytes, err := proto.Marshal(fileMetadata)
	if err != nil {
		return nil, err
	}
	metadataReader := bytes.NewReader(metadataBytes)
	metadataResult := new(bytes.Buffer)
	err = file.DownloadWorkspaceFile(apiClient, ownerId, projectId, workspaceId, changeId, ".jamfilelist", metadataReader, metadataResult)
	if err != nil {
		return nil, err
	}

	remoteFileMetadata := &jampb.FileMetadata{}
	err = proto.Unmarshal(metadataResult.Bytes(), remoteFileMetadata)
	if err != nil {
		return nil, err
	}

	fileMetadataDiff := make(map[string]*jampb.FileMetadataDiff_FileDiff, len(remoteFileMetadata.GetFiles()))
	for remoteFilePath := range remoteFileMetadata.GetFiles() {
		fileMetadataDiff[remoteFilePath] = &jampb.FileMetadataDiff_FileDiff{
			Type: jampb.FileMetadataDiff_Delete,
		}
	}

	for filePath, file := range fileMetadata.GetFiles() {
		var diffFile *jampb.File
		diffType := jampb.FileMetadataDiff_Delete
		remoteFile, found := remoteFileMetadata.GetFiles()[filePath]
		if found && proto.Equal(file, remoteFile) {
			diffType = jampb.FileMetadataDiff_NoOp
		} else if found {
			diffFile = file
			diffType = jampb.FileMetadataDiff_Update
		} else {
			diffFile = file
			diffType = jampb.FileMetadataDiff_Create
		}

		fileMetadataDiff[filePath] = &jampb.FileMetadataDiff_FileDiff{
			Type: diffType,
			File: diffFile,
		}
	}

	return &jampb.FileMetadataDiff{
		Diffs: fileMetadataDiff,
	}, err
}
