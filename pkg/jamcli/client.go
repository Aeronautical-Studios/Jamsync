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
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding/gzip"
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

	var wg sync.WaitGroup
	files := make(map[string]*jampb.File, numEntries)
	filesMutex := sync.Mutex{}

	var walkDir func(dir string) // needed for goroutine
	walkDir = func(dir string) {
		defer wg.Done()

		hasher := xxh3.New()
		visit := func(path string, f os.FileInfo, err error) error {
			if path == "." {
				return nil
			}
			path = filepath.ToSlash(path)
			if ignorer.Match(path) {
				return nil
			}

			if f.IsDir() && path != dir {
				wg.Add(1)
				go walkDir(path)
				filesMutex.Lock()
				files[path] = &jampb.File{
					Dir: true,
				}
				filesMutex.Unlock()
				return filepath.SkipDir
			} else if path == dir {
				return nil
			}

			data, err := os.ReadFile(path)
			if err != nil {
				fmt.Println("Could not read ", path, "(jam does not support symlinks yet)", err)
				return nil
			}
			hasher.Reset()
			hasher.Write(data)
			bytes := hasher.Sum128().Bytes()

			if path != "" {
				filesMutex.Lock()
				files[path] = &jampb.File{
					Dir:  false,
					Hash: bytes[:],
				}
				filesMutex.Unlock()
			}

			return nil
		}

		filepath.Walk(dir, visit)
	}

	wg.Add(1)
	walkDir(".")
	wg.Wait()

	return &jampb.FileMetadata{
		Files: files,
	}
}

func pathToHash(path string) []byte {
	h := xxh3.Hash128([]byte(path)).Bytes()
	return h[:]
}

func pushFileListDiffWorkspace(conn *grpc.ClientConn, ownerUsername string, projectId uint64, workspaceId uint64, currChangeId uint64, fileMetadata *jampb.FileMetadata, fileMetadataDiff *jampb.FileMetadataDiff) error {
	ctx := context.Background()

	numFiles := 0
	for _, diff := range fileMetadataDiff.GetDiffs() {
		if diff.GetType() != jampb.FileMetadataDiff_NoOp && diff.GetType() != jampb.FileMetadataDiff_Delete && !diff.GetFile().GetDir() {
			numFiles += 1
		}
	}
	apiClient := jampb.NewJamHubClient(conn)
	tokenResp, err := apiClient.GetOperationStreamToken(ctx, &jampb.GetOperationStreamTokenRequest{
		OwnerUsername: ownerUsername,
		ProjectId:     projectId,
		WorkspaceId:   workspaceId,
	})
	if err != nil {
		panic(err)
	}

	bar := progressbar.DefaultBytes(-1, "Uploading")

	pathHashToPath := make(map[string]string)
	pathHashes := make([][]byte, 0, numFiles)
	for path, diff := range fileMetadataDiff.GetDiffs() {
		if diff.GetType() != jampb.FileMetadataDiff_NoOp && diff.GetType() != jampb.FileMetadataDiff_Delete && !diff.GetFile().GetDir() {
			pathHashes = append(pathHashes, pathToHash(path))
			pathHashToPath[string(pathToHash(path))] = path
		}
	}

	pathHashes = append(pathHashes, pathToHash(".jamfilelist"))
	pathHashToPath[string(pathToHash(".jamfilelist"))] = ".jamfilelist"

	fileHashesResp, err := apiClient.ReadWorkspaceFileHashes(ctx, &jampb.ReadWorkspaceFileHashesRequest{
		OwnerUsername: ownerUsername,
		ProjectId:     projectId,
		WorkspaceId:   workspaceId,
		ChangeId:      currChangeId,
		PathHashes:    pathHashes,
	})
	if err != nil {
		panic(err)
	}

	sourceChunker, err := fastcdc.NewJamChunker(fastcdc.DefaultOpts)
	if err != nil {
		panic(err)
	}

	operationStream, err := apiClient.WriteWorkspaceOperationsStream(ctx, grpc.UseCompressor(gzip.Name))
	if err != nil {
		panic(err)
	}

	operations := make([]*jampb.FileWriteOperation, 0, numFiles)
	var wg sync.WaitGroup
	written := 0
	for _, job := range fileHashesResp.Hashes {
		path := pathHashToPath[string(job.PathHash)]
		var file *os.File
		if path == ".jamfilelist" {
			metadataBytes, err := proto.Marshal(fileMetadata)
			if err != nil {
				return err
			}
			sourceChunker.SetChunkerReader(bytes.NewReader(metadataBytes))
		} else {
			file, err = os.Open(path)
			if err != nil {
				return err
			}
			sourceChunker.SetChunkerReader(file)
		}

		err = sourceChunker.CreateDelta(job.Hashes, func(chunk *jampb.Chunk) error {
			written = written + len(chunk.Data)
			if written > 1024*1024 {
				wg.Add(1)
				writeOps := make([]*jampb.FileWriteOperation, len(operations))
				copy(writeOps, operations)
				go func(writeOps []*jampb.FileWriteOperation) {
					_, err := jampb.NewJamHubClient(conn).WriteWorkspaceOperations(ctx, &jampb.WriteWorkspaceOperationsRequest{
						OperationToken: tokenResp.Token,
						Operations:     writeOps,
					}, grpc.UseCompressor(gzip.Name))
					if err != nil {
						panic(err)
					}
					for _, op := range writeOps {
						bar.Write(op.Chunk.Data)
					}
					wg.Done()
				}(writeOps)
				written = 0
				operations = operations[:0]
			}
			operations = append(operations, &jampb.FileWriteOperation{
				PathHash: pathToHash(path),
				Chunk:    chunk,
			})
			written += len(chunk.Data)
			return nil
		})
		if err != nil {
			panic(err)
		}

		if file != nil {
			err = file.Close()
			if err != nil {
				panic(err)
			}
			file = nil
		}
	}

	wg.Add(1)
	go func() {
		_, err = jampb.NewJamHubClient(conn).WriteWorkspaceOperations(ctx, &jampb.WriteWorkspaceOperationsRequest{
			OperationToken: tokenResp.Token,
			Operations:     operations,
		}, grpc.UseCompressor(gzip.Name))
		if err != nil {
			panic(err)
		}
		for _, op := range operations {
			bar.Write(op.Chunk.Data)
		}
		wg.Done()
	}()

	wg.Wait()
	bar.Finish()

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

	bar := progressbar.Default(numFiles, "Downloading")
	for path, diff := range fileMetadataDiff.GetDiffs() {
		if diff.GetType() != jampb.FileMetadataDiff_NoOp && !diff.GetFile().GetDir() {
			if diff.GetType() == jampb.FileMetadataDiff_Delete {
				err := os.Remove(path)
				if err != nil {
					panic(err)
				}
				bar.ChangeMax(bar.GetMax() - 1)
			} else {
				currFile, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE, 0755)
				if err != nil {
					panic(err)
				}

				targetChunker, err := fastcdc.NewJamChunker(fastcdc.DefaultOpts)
				if err != nil {
					panic(err)
				}
				targetChunker.SetChunkerReader(currFile)

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
				bar.Add(1)
			}
		}
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

	targetChunker, err := fastcdc.NewJamChunker(fastcdc.DefaultOpts)
	if err != nil {
		return err
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

				targetChunker.SetChunkerReader(currFile)

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
