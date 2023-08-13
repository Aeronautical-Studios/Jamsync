package jamgrpc

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"io"
	"os"

	"github.com/zdgeier/jam/gen/jampb"
	"github.com/zdgeier/jam/pkg/fastcdc"
	"github.com/zdgeier/jam/pkg/jamgrpc/serverauth"
	"github.com/zdgeier/jam/pkg/jamstores/file"
	"github.com/zdgeier/jam/pkg/jamstores/merger"
	"github.com/zeebo/xxh3"
	"google.golang.org/protobuf/proto"
)

func (s JamHub) CreateWorkspace(ctx context.Context, in *jampb.CreateWorkspaceRequest) (*jampb.CreateWorkspaceResponse, error) {
	userId, err := serverauth.ParseIdFromCtx(ctx)
	if err != nil {
		return nil, err
	}

	currentUsername, err := s.db.GetUsername(userId)
	if err != nil {
		return nil, err
	}

	accessible, err := s.ProjectIdAccessible(in.GetOwnerUsername(), in.GetProjectId(), currentUsername)
	if err != nil {
		return nil, err
	}

	if !accessible {
		return nil, errors.New("not an owner or collaborator of this project")
	}

	db, err := s.projectstore.GetLocalProjectDB(in.GetOwnerUsername(), in.GetProjectId())
	if err != nil {
		return nil, err
	}
	defer db.Close()

	maxCommitId, err := s.projectstore.MaxCommitId(db)
	if err != nil {
		return nil, err
	}

	workspaceId, err := s.projectstore.AddWorkspace(db, in.GetWorkspaceName(), maxCommitId)
	if err != nil {
		return nil, err
	}

	return &jampb.CreateWorkspaceResponse{
		WorkspaceId: workspaceId,
	}, nil
}

func (s JamHub) GetWorkspaceName(ctx context.Context, in *jampb.GetWorkspaceNameRequest) (*jampb.GetWorkspaceNameResponse, error) {
	userId, err := serverauth.ParseIdFromCtx(ctx)
	if err != nil {
		return nil, err
	}

	currentUsername, err := s.db.GetUsername(userId)
	if err != nil {
		return nil, err
	}

	accessible, err := s.ProjectIdAccessible(in.GetOwnerUsername(), in.GetProjectId(), currentUsername)
	if err != nil {
		return nil, err
	}

	if !accessible {
		return nil, errors.New("not an owner or collaborator of this project")
	}
	db, err := s.projectstore.GetLocalProjectDB(in.GetOwnerUsername(), in.GetProjectId())
	if err != nil {
		return nil, err
	}
	defer db.Close()

	workspaceName, err := s.projectstore.GetWorkspaceNameById(db, in.GetWorkspaceId())
	if err != nil {
		return nil, err
	}

	return &jampb.GetWorkspaceNameResponse{
		WorkspaceName: workspaceName,
	}, nil
}

func (s JamHub) GetWorkspaceId(ctx context.Context, in *jampb.GetWorkspaceIdRequest) (*jampb.GetWorkspaceIdResponse, error) {
	userId, err := serverauth.ParseIdFromCtx(ctx)
	if err != nil {
		return nil, err
	}

	currentUsername, err := s.db.GetUsername(userId)
	if err != nil {
		return nil, err
	}

	accessible, err := s.ProjectIdAccessible(in.GetOwnerUsername(), in.GetProjectId(), currentUsername)
	if err != nil {
		return nil, err
	}

	if !accessible {
		return nil, errors.New("not an owner or collaborator of this project")
	}
	db, err := s.projectstore.GetLocalProjectDB(in.GetOwnerUsername(), in.GetProjectId())
	if err != nil {
		return nil, err
	}
	defer db.Close()

	workspaceId, err := s.projectstore.GetWorkspaceIdByName(db, in.GetWorkspaceName())
	if err != nil {
		return nil, err
	}

	return &jampb.GetWorkspaceIdResponse{
		WorkspaceId: workspaceId,
	}, nil
}

func (s JamHub) GetWorkspaceCurrentChange(ctx context.Context, in *jampb.GetWorkspaceCurrentChangeRequest) (*jampb.GetWorkspaceCurrentChangeResponse, error) {
	userId, err := serverauth.ParseIdFromCtx(ctx)
	if err != nil {
		return nil, err
	}

	currentUsername, err := s.db.GetUsername(userId)
	if err != nil {
		return nil, err
	}

	accessible, err := s.ProjectIdAccessible(in.GetOwnerUsername(), in.GetProjectId(), currentUsername)
	if err != nil {
		return nil, err
	}

	if !accessible {
		return nil, errors.New("not an owner or collaborator of this project")
	}
	db, err := s.projectstore.GetLocalProjectDB(in.GetOwnerUsername(), in.GetProjectId())
	if err != nil {
		return nil, err
	}
	defer db.Close()

	changeId, err := s.projectstore.MaxWorkspaceChangeId(db, in.GetOwnerUsername(), in.GetProjectId(), in.GetWorkspaceId())
	if err != nil {
		return nil, err
	}

	return &jampb.GetWorkspaceCurrentChangeResponse{
		ChangeId: changeId,
	}, nil
}

func (s JamHub) ListWorkspaces(ctx context.Context, in *jampb.ListWorkspacesRequest) (*jampb.ListWorkspacesResponse, error) {
	userId, err := serverauth.ParseIdFromCtx(ctx)
	if err != nil {
		return nil, err
	}

	currentUsername, err := s.db.GetUsername(userId)
	if err != nil {
		return nil, err
	}

	accessible, err := s.ProjectIdAccessible(in.GetOwnerUsername(), in.GetProjectId(), currentUsername)
	if err != nil {
		return nil, err
	}

	if !accessible {
		return nil, errors.New("not an owner or collaborator of this project")
	}

	db, err := s.projectstore.GetLocalProjectDB(in.GetOwnerUsername(), in.GetProjectId())
	if err != nil {
		return nil, err
	}
	defer db.Close()

	workspaces, err := s.projectstore.ListWorkspaces(db)
	if err != nil {
		return nil, err
	}

	baseCommitIds := make(map[uint64]uint64)
	for _, workspaceId := range workspaces {
		baseCommitId, err := s.projectstore.GetWorkspaceBaseCommitId(db, workspaceId)
		if err != nil {
			return nil, err
		}
		baseCommitIds[workspaceId] = baseCommitId
	}

	return &jampb.ListWorkspacesResponse{
		Workspaces:    workspaces,
		BaseCommitIds: baseCommitIds,
	}, nil
}

func (s JamHub) GetOperationStreamToken(ctx context.Context, in *jampb.GetOperationStreamTokenRequest) (*jampb.GetOperationStreamTokenResponse, error) {
	userId, err := serverauth.ParseIdFromCtx(ctx)
	if err != nil {
		return nil, err
	}

	currentUsername, err := s.db.GetUsername(userId)
	if err != nil {
		return nil, err
	}

	accessible, err := s.ProjectIdAccessible(in.GetOwnerUsername(), in.GetProjectId(), currentUsername)
	if err != nil {
		return nil, err
	}

	if !accessible {
		return nil, errors.New("not an owner or collaborator of this project")
	}

	token := make([]byte, 64)
	_, err = rand.Read(token)
	if err != nil {
		return nil, err
	}
	db, err := s.projectstore.GetLocalProjectDB(in.GetOwnerUsername(), in.GetProjectId())
	if err != nil {
		return nil, err
	}
	defer db.Close()

	newChangeId, err := s.projectstore.AddChange(db, in.GetOwnerUsername(), in.GetProjectId(), in.GetWorkspaceId())
	if err != nil {
		return nil, err
	}

	err = s.db.AddOperationStreamToken(in.GetOwnerUsername(), in.GetProjectId(), in.GetWorkspaceId(), newChangeId, token)
	if err != nil {
		return nil, err
	}

	return &jampb.GetOperationStreamTokenResponse{
		Token:       token,
		NewChangeId: newChangeId,
	}, nil
}

func (s JamHub) WriteWorkspaceOperationsStream(srv jampb.JamHub_WriteWorkspaceOperationsStreamServer) error {
	var (
		projectOwner                     string
		projectId, workspaceId, changeId uint64
	)
	_, err := serverauth.ParseIdFromCtx(srv.Context())
	if err != nil {
		return err
	}

	var projectDB *sql.DB
	for {
		in, err := srv.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if projectOwner == "" {
			if in.OperationToken == nil {
				return errors.New("first chunk must contain operation token")
			}
			projectOwner, projectId, workspaceId, changeId, err = s.db.GetOperationStreamTokenInfo(in.GetOperationToken())
			if err != nil {
				return err
			}
			projectDB, err = s.projectstore.GetLocalProjectDB(projectOwner, projectId)
			if err != nil {
				return err
			}
			defer projectDB.Close()
		}

		for _, op := range in.Operations {
			if op.Chunk != nil {
				conn, err := s.workspacedatastore.GetLocalDB(projectOwner, projectId, workspaceId, op.PathHash)
				if err != nil {
					return err
				}
				err = s.workspacedatastore.Write(conn, op.Chunk.Hash, op.Chunk.Data)
				if err != nil {
					return err
				}
				err = conn.Close()
				if err != nil {
					return err
				}
			}

			err = s.projectstore.InsertWorkspaceChunkHash(projectDB, workspaceId, changeId, op.PathHash, &jampb.ChunkHash{
				Hash:   op.Chunk.Hash,
				Offset: op.Chunk.Offset,
				Length: op.Chunk.Length,
			})
			if err != nil {
				return err
			}
		}
	}

	return srv.SendAndClose(&jampb.WriteOperationStreamResponse{})
}

func (s JamHub) ReadWorkspaceFileHashes(ctx context.Context, in *jampb.ReadWorkspaceFileHashesRequest) (*jampb.ReadWorkspaceFileHashesResponse, error) {
	userId, err := serverauth.ParseIdFromCtx(ctx)
	if err != nil {
		return nil, err
	}

	currentUsername, err := s.db.GetUsername(userId)
	if err != nil {
		return nil, err
	}

	accessible, err := s.ProjectIdAccessible(in.GetOwnerUsername(), in.GetProjectId(), currentUsername)
	if err != nil {
		return nil, err
	}

	if !accessible {
		return nil, errors.New("not an owner or collaborator of this project")
	}

	hashLists := make([]*jampb.HashList, 0, len(in.PathHashes))
	for _, pathHash := range in.PathHashes {
		// optimization: check if files exists, if not, return empty hash list
		if !s.workspacedatastore.LocalDBExists(in.OwnerUsername, in.ProjectId, in.WorkspaceId, pathHash) && !s.commitdatastore.LocalDBExists(in.OwnerUsername, in.ProjectId, pathHash) {
			hashLists = append(hashLists, &jampb.HashList{
				PathHash: pathHash,
			})
			continue
		}

		workspaceConn, err := s.workspacedatastore.GetLocalDB(in.OwnerUsername, in.ProjectId, in.WorkspaceId, pathHash)
		if err != nil {
			panic(err)
		}

		commitConn, err := s.commitdatastore.GetLocalDB(in.OwnerUsername, in.ProjectId, pathHash)
		if err != nil {
			panic(err)
		}

		hashList, err := s.workspacedatastore.GetChunkHashes(workspaceConn, pathHash)
		if err != nil {
			panic(err)
		}

		committedHashList, err := s.commitdatastore.GetChunkHashes(commitConn, pathHash)
		if err != nil {
			panic(err)
		}

		hashMap := make(map[uint64][]byte)
		for _, hash := range hashList {
			hashMap[hash] = nil
		}
		for _, hash := range committedHashList {
			hashMap[hash] = nil
		}

		err = workspaceConn.Close()
		if err != nil {
			panic(err)
		}
		err = commitConn.Close()
		if err != nil {
			panic(err)
		}

		hashLists = append(hashLists, &jampb.HashList{
			PathHash: pathHash,
			Hashes:   hashMap,
		})
	}

	return &jampb.ReadWorkspaceFileHashesResponse{
		Hashes: hashLists,
	}, nil
}

func (s JamHub) ReadWorkspaceFile(in *jampb.ReadWorkspaceFileRequest, srv jampb.JamHub_ReadWorkspaceFileServer) error {
	userId, err := serverauth.ParseIdFromCtx(srv.Context())
	if err != nil {
		return err
	}

	currentUsername, err := s.db.GetUsername(userId)
	if err != nil {
		return err
	}

	accessible, err := s.ProjectIdAccessible(in.GetOwnerUsername(), in.GetProjectId(), currentUsername)
	if err != nil {
		return err
	}

	if !accessible {
		return errors.New("not an owner or collaborator of this project")
	}

	db, err := s.projectstore.GetLocalProjectDB(in.GetOwnerUsername(), in.GetProjectId())
	if err != nil {
		return err
	}
	defer db.Close()

	chunkHashes, err := s.projectstore.ListWorkspaceChunkHashes(db, in.WorkspaceId, in.ChangeId, in.PathHash)
	if err != nil {
		return err
	}

	hashes := make(map[uint64][]byte)
	for _, chunkHash := range chunkHashes {
		hashes[chunkHash.Hash] = nil
	}

	workspaceConn, err := s.workspacedatastore.GetLocalDB(in.OwnerUsername, in.ProjectId, in.WorkspaceId, in.PathHash)
	if err != nil {
		return err
	}
	defer workspaceConn.Close()

	commitConn, err := s.commitdatastore.GetLocalDB(in.OwnerUsername, in.ProjectId, in.PathHash)
	if err != nil {
		return err
	}
	defer commitConn.Close()

	for _, chunk := range chunkHashes {
		if _, ok := in.LocalChunkHashes[chunk.Hash]; ok {
			err = srv.Send(&jampb.FileReadOperation{
				PathHash: in.PathHash,
				Chunk: &jampb.Chunk{
					Hash:   chunk.Hash,
					Offset: chunk.Offset,
					Length: chunk.Length,
				},
			})
			if err != nil {
				return err
			}
		} else {
			data, err := s.workspacedatastore.Read(workspaceConn, chunk.Hash)
			if err != nil || len(data) == 0 {
				data, err = s.commitdatastore.Read(commitConn, chunk.Hash)
				if err != nil {
					return err
				}
			}
			err = srv.Send(&jampb.FileReadOperation{
				PathHash: in.PathHash,
				Chunk: &jampb.Chunk{
					Hash:   chunk.Hash,
					Offset: chunk.Offset,
					Length: chunk.Length,
					Data:   data,
				},
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s JamHub) DeleteWorkspace(ctx context.Context, in *jampb.DeleteWorkspaceRequest) (*jampb.DeleteWorkspaceResponse, error) {
	userId, err := serverauth.ParseIdFromCtx(ctx)
	if err != nil {
		return nil, err
	}

	currentUsername, err := s.db.GetUsername(userId)
	if err != nil {
		return nil, err
	}

	accessible, err := s.ProjectIdAccessible(in.GetOwnerUsername(), in.GetProjectId(), currentUsername)
	if err != nil {
		return nil, err
	}

	if !accessible {
		return nil, errors.New("not an owner or collaborator of this project")
	}

	err = s.workspacedatastore.DeleteWorkspace(in.GetOwnerUsername(), in.GetProjectId(), in.GetWorkspaceId())
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	db, err := s.projectstore.GetLocalProjectDB(in.GetOwnerUsername(), in.GetProjectId())
	if err != nil {
		return nil, err
	}
	defer db.Close()

	err = s.projectstore.DeleteWorkspace(db, in.GetWorkspaceId())
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	err = s.projectstore.DeleteWorkspace(db, in.GetWorkspaceId())
	if err != nil {
		return nil, err
	}

	return &jampb.DeleteWorkspaceResponse{}, nil
}

func (s JamHub) regenCommittedFile(db *sql.DB, ownerUsername string, projectId, commitId uint64, pathHash []byte, file *bytes.Buffer) error {
	chunkHashes, err := s.projectstore.ListCommitChunkHashes(db, commitId, pathHash)
	if err != nil {
		return err
	}

	offset := uint64(0)
	conn, err := s.commitdatastore.GetLocalDB(ownerUsername, projectId, pathHash)
	if err != nil {
		return err
	}
	defer conn.Close()

	for _, chunkHash := range chunkHashes {
		data, err := s.commitdatastore.Read(conn, chunkHash.Hash)
		if err != nil {
			return err
		}
		n, err := file.Write(data)
		if err != nil {
			return err
		}
		if n != len(data) || n != int(chunkHash.Length) {
			return errors.New("failed to write all data")
		} else if offset != chunkHash.Offset {
			return errors.New("invalid offset while writing data")
		}
		offset += chunkHash.Length
	}
	return nil
}

func (s JamHub) regenWorkspaceFile(db *sql.DB, ownerUsername string, projectId, workspaceId, changeId uint64, pathHash []byte, file *bytes.Buffer) error {
	chunkHashes, err := s.projectstore.ListWorkspaceChunkHashes(db, workspaceId, changeId, pathHash)
	if err != nil {
		return err
	}

	offset := uint64(0)
	workspaceConn, err := s.workspacedatastore.GetLocalDB(ownerUsername, projectId, workspaceId, pathHash)
	if err != nil {
		return err
	}
	defer workspaceConn.Close()

	commitConn, err := s.commitdatastore.GetLocalDB(ownerUsername, projectId, pathHash)
	if err != nil {
		return err
	}
	defer commitConn.Close()

	for _, chunkHash := range chunkHashes {
		data, err := s.workspacedatastore.Read(workspaceConn, chunkHash.Hash)
		if err != nil || len(data) == 0 {
			data, err = s.commitdatastore.Read(commitConn, chunkHash.Hash)
			if err != nil {
				return err
			}
		}
		n, err := file.Write(data)
		if err != nil {
			return err
		}
		if n != len(data) || n != int(chunkHash.Length) {
			return errors.New("failed to write all data")
		} else if offset != chunkHash.Offset {
			return errors.New("invalid offset while writing data")
		}
		offset += chunkHash.Length
	}
	return nil
}

func (s JamHub) UpdateWorkspace(ctx context.Context, in *jampb.UpdateWorkspaceRequest) (*jampb.UpdateWorkspaceResponse, error) {
	userId, err := serverauth.ParseIdFromCtx(ctx)

	if err != nil {
		return nil, err
	}

	if in.GetOwnerUsername() == "" {
		return nil, errors.New("must provide owner id")
	}

	username, err := s.db.GetUsername(userId)

	if err != nil {
		return nil, err
	}

	accessible, err := s.ProjectIdAccessible(in.GetOwnerUsername(), in.GetProjectId(), username)

	if err != nil {
		return nil, err
	}

	if !accessible {
		return nil, errors.New("must be owner or collaborator to merge")
	}

	db, err := s.projectstore.GetLocalProjectDB(in.GetOwnerUsername(), in.GetProjectId())
	if err != nil {
		return nil, err
	}
	defer db.Close()
	changedWorkspacePathHashes, err := s.projectstore.ListWorkspaceChangedPathHashes(db, in.GetWorkspaceId())
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	workspaceBaseCommitId, err := s.projectstore.GetWorkspaceBaseCommitId(db, in.GetWorkspaceId())
	if err != nil {
		return nil, err
	}

	maxCommitId, err := s.projectstore.MaxCommitId(db)
	if err != nil {
		return nil, err
	}

	if workspaceBaseCommitId == maxCommitId {
		return nil, errors.New("already up-to-date")
	}

	changedCommitPathHashes, err := s.projectstore.ListChangedPathHashesFromCommit(db, workspaceBaseCommitId)
	if err != nil {
		return nil, err
	}

	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	bothChangedPathHashes := make(map[string]interface{})
	for workspacePathHash := range changedWorkspacePathHashes {
		if _, ok := changedCommitPathHashes[workspacePathHash]; ok {
			bothChangedPathHashes[string(workspacePathHash)] = nil
		}
	}

	maxChangeId, err := s.projectstore.MaxWorkspaceChangeId(db, in.GetOwnerUsername(), in.GetProjectId(), in.GetWorkspaceId())
	if err != nil {
		return nil, err
	}

	fileListBuffer := bytes.NewBuffer(nil)
	err = s.regenWorkspaceFile(db, in.GetOwnerUsername(), in.GetProjectId(), in.GetWorkspaceId(), maxChangeId, file.PathToHash(".jamfilelist"), fileListBuffer)
	if err != nil {
		return nil, err
	}

	fileList, err := io.ReadAll(fileListBuffer)
	if err != nil {
		return nil, err
	}

	fileMetadata := &jampb.FileMetadata{}
	err = proto.Unmarshal(fileList, fileMetadata)
	if err != nil {
		return nil, err
	}

	newChangeId, err := s.projectstore.AddChange(db, in.GetOwnerUsername(), in.GetProjectId(), in.WorkspaceId)
	if err != nil {
		return nil, err
	}

	oldCommittedFile := bytes.NewBuffer([]byte{})
	currentCommittedFile := bytes.NewBuffer([]byte{})
	workspaceFile := bytes.NewBuffer([]byte{})
	conflicts := make([]string, 0)
	makeDiff := func(pathHashes <-chan []byte, results chan<- error) {
		newChunker, err := fastcdc.NewJamChunker(fastcdc.DefaultOpts)
		if err != nil {
			results <- err
		}
		for pathHash := range pathHashes {
			var newContent *bytes.Reader
			if bytes.Equal(pathHash, file.PathToHash(".jamfilelist")) {
				fileMetadataBytes, err := proto.Marshal(fileMetadata)
				if err != nil {
					results <- err
					continue
				}
				newContent = bytes.NewReader(fileMetadataBytes)
			} else {
				err := s.regenCommittedFile(db, in.GetOwnerUsername(), in.GetProjectId(), workspaceBaseCommitId, pathHash, oldCommittedFile)
				if err != nil {
					results <- err
					continue
				}

				err = s.regenCommittedFile(db, in.GetOwnerUsername(), in.GetProjectId(), maxCommitId, pathHash, currentCommittedFile)
				if err != nil {
					results <- err
					continue
				}

				err = s.regenWorkspaceFile(db, in.GetOwnerUsername(), in.GetProjectId(), in.GetWorkspaceId(), maxChangeId, pathHash, workspaceFile)
				if err != nil {
					results <- err
					continue
				}

				var specificFilePath string
				var specificFileMetadata *jampb.File
				for k, v := range fileMetadata.Files {
					if bytes.Equal(file.PathToHash(k), pathHash) {
						specificFilePath = k
						specificFileMetadata = v
						break
					}
				}

				mergedFile, err := merger.Merge(in.GetOwnerUsername(), in.GetProjectId(), specificFilePath, oldCommittedFile, workspaceFile, currentCommittedFile)
				if err != nil {
					results <- err
					continue
				}

				mergedData, _ := io.ReadAll(mergedFile)
				b := xxh3.Hash128(mergedData).Bytes()
				mergedFile.Seek(0, 0)

				conflicts = append(conflicts, specificFilePath)
				specificFileMetadata.Hash = b[:]

				newContent = mergedFile
			}
			newChunker.SetChunkerReader(newContent)

			newChunks := make([]*jampb.Chunk, 0)
			err = newChunker.CreateDelta(nil, func(chunk *jampb.Chunk) error {
				newChunks = append(newChunks, chunk)
				return nil
			})
			if err != nil {
				results <- err
				continue
			}

			commitConn, err := s.commitdatastore.GetLocalDB(in.GetOwnerUsername(), in.GetProjectId(), pathHash)
			if err != nil {
				results <- err
				continue
			}
			workspaceConn, err := s.workspacedatastore.GetLocalDB(in.GetOwnerUsername(), in.GetProjectId(), in.GetWorkspaceId(), pathHash)
			if err != nil {
				results <- err
				continue
			}

			newChunkHashes := make([]*jampb.ChunkHash, 0)
			for _, newChunk := range newChunks {
				if !s.workspacedatastore.HashExists(workspaceConn, newChunk.Hash) {
					err := s.workspacedatastore.Write(workspaceConn, newChunk.Hash, newChunk.Data)
					if err != nil {
						results <- err
						continue
					}
				}
				newChunkHashes = append(newChunkHashes, &jampb.ChunkHash{
					Hash:   newChunk.Hash,
					Offset: newChunk.Offset,
					Length: newChunk.Length,
				})
			}
			commitConn.Close()
			workspaceConn.Close()

			err = s.projectstore.InsertWorkspaceChunkHashes(db, in.WorkspaceId, newChangeId, pathHash, newChunkHashes)
			if err != nil {
				results <- err
				continue
			}

			results <- nil
		}
	}

	pathHashes := make(chan []byte)
	results := make(chan error)
	for i := 0; i < 64; i++ {
		go makeDiff(pathHashes, results)
	}

	numJobs := 0
	for k := range bothChangedPathHashes {
		if bytes.Equal([]byte(k), file.PathToHash(".jamfilelist")) {
			// Ignore file list since we'll add it back once everything is done
			continue
		}
		pathHashes <- []byte(k)
		numJobs++
	}

	completed := 0

	for i := 0; i < numJobs; i++ {
		e := <-results
		if e != nil {
			return nil, err
		}
		completed += 1

		if completed == len(bothChangedPathHashes)-1 {
			pathHashes <- []byte(file.PathToHash(".jamfilelist"))
		}

		if completed == len(bothChangedPathHashes) {
			close(pathHashes)
			close(results)
		}
	}

	err = s.projectstore.UpdateWorkspaceBaseCommit(db, in.GetWorkspaceId(), maxCommitId)
	if err != nil {
		return nil, err
	}

	return &jampb.UpdateWorkspaceResponse{
		Conflicts: conflicts,
	}, nil
}

func (s JamHub) AddChange(ctx context.Context, in *jampb.AddChangeRequest) (*jampb.AddChangeResponse, error) {
	userId, err := serverauth.ParseIdFromCtx(ctx)
	if err != nil {
		return nil, err
	}

	if in.GetOwnerUsername() == "" {
		return nil, errors.New("must provide owner id")
	}
	username, err := s.db.GetUsername(userId)
	if err != nil {
		return nil, err
	}

	accessible, err := s.ProjectIdAccessible(in.GetOwnerUsername(), in.GetProjectId(), username)
	if err != nil {
		return nil, err
	}
	if !accessible {
		return nil, errors.New("must be owner or collaborator to merge")
	}

	db, err := s.projectstore.GetLocalProjectDB(in.GetOwnerUsername(), in.GetProjectId())
	if err != nil {
		return nil, err
	}
	defer db.Close()
	newChangeId, err := s.projectstore.AddChange(db, in.GetOwnerUsername(), in.GetProjectId(), in.GetWorkspaceId())
	if err != nil {
		return nil, err
	}

	return &jampb.AddChangeResponse{
		ChangeId: newChangeId,
	}, nil
}

func (s JamHub) MergeWorkspace(ctx context.Context, in *jampb.MergeWorkspaceRequest) (*jampb.MergeWorkspaceResponse, error) {
	userId, err := serverauth.ParseIdFromCtx(ctx)
	if err != nil {
		return nil, err
	}

	if in.GetOwnerUsername() == "" {
		return nil, errors.New("must provide owner id")
	}
	username, err := s.db.GetUsername(userId)
	if err != nil {
		return nil, err
	}

	accessible, err := s.ProjectIdAccessible(in.GetOwnerUsername(), in.GetProjectId(), username)
	if err != nil {
		return nil, err
	}
	if !accessible {
		return nil, errors.New("must be owner or collaborator to merge")
	}
	db, err := s.projectstore.GetLocalProjectDB(in.GetOwnerUsername(), in.GetProjectId())
	if err != nil {
		return nil, err
	}
	defer db.Close()

	baseCommitId, err := s.projectstore.GetWorkspaceBaseCommitId(db, in.GetWorkspaceId())
	if err != nil {
		return nil, err
	}

	prevCommitId, err := s.projectstore.MaxCommitId(db)
	if err != nil {
		return nil, err
	}
	if baseCommitId != prevCommitId {
		return nil, errors.New("workspace is not up to date with latest commit")
	}

	changedPathHashes, err := s.projectstore.ListWorkspaceChangedPathHashes(db, in.GetWorkspaceId())
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if len(changedPathHashes) == 0 {
		return &jampb.MergeWorkspaceResponse{CommitId: prevCommitId}, nil
	}
	maxChangeId, err := s.projectstore.MaxWorkspaceChangeId(db, in.GetOwnerUsername(), in.GetProjectId(), in.GetWorkspaceId())
	if err != nil {
		return nil, err
	}

	newCommitId, err := s.projectstore.AddCommit(db)
	if err != nil {
		return nil, err
	}

	type result struct {
		pathHash    []byte
		chunkHashes []*jampb.ChunkHash
	}
	worker := func(pathHashes chan []byte, results chan result) {
		for pathHash := range pathHashes {
			workspaceChunkHashes, err := s.projectstore.ListWorkspaceChunkHashes(db, in.GetWorkspaceId(), maxChangeId, pathHash)
			if err != nil {
				panic(err)
			}

			workspaceConn, err := s.workspacedatastore.GetLocalDB(in.OwnerUsername, in.ProjectId, in.WorkspaceId, pathHash)
			if err != nil {
				panic(err)
			}

			commitConn, err := s.commitdatastore.GetLocalDB(in.OwnerUsername, in.ProjectId, pathHash)
			if err != nil {
				panic(err)
			}

			for _, chunkHash := range workspaceChunkHashes {
				if !s.commitdatastore.HashExists(commitConn, chunkHash.Hash) {
					data, err := s.workspacedatastore.Read(workspaceConn, chunkHash.Hash)
					if err != nil {
						panic(err)
					}
					err = s.commitdatastore.Write(commitConn, chunkHash.Hash, data)
					if err != nil {
						panic(err)
					}
				}
			}
			results <- result{
				pathHash:    pathHash,
				chunkHashes: workspaceChunkHashes,
			}
			workspaceConn.Close()
			commitConn.Close()
		}
	}

	pathHashes := make(chan []byte, len(changedPathHashes))
	results := make(chan result, len(changedPathHashes))
	for i := 0; i < 8 && i <= len(changedPathHashes)/10+1; i++ {
		go worker(pathHashes, results)
	}

	go func() {
		for changedPathHash := range changedPathHashes {
			pathHashes <- []byte(changedPathHash)
		}
		close(pathHashes)
	}()

	newChunkHashes := make(map[string][]*jampb.ChunkHash, len(changedPathHashes))
	for i := 0; i < len(changedPathHashes); i++ {
		res := <-results
		newChunkHashes[string(res.pathHash)] = res.chunkHashes
	}

	for pathHash, workspaceChunkHashes := range newChunkHashes {
		err = s.projectstore.InsertCommitChunkHashes(db, newCommitId, []byte(pathHash), workspaceChunkHashes)
		if err != nil {
			return nil, err
		}
	}

	err = s.projectstore.UpdateWorkspaceBaseCommit(db, in.GetWorkspaceId(), newCommitId)
	if err != nil {
		return nil, err
	}

	return &jampb.MergeWorkspaceResponse{
		CommitId: newCommitId,
	}, nil
}
