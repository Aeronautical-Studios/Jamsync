package jamgrpc

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"io"
	"os"
	"sort"
	"strconv"

	"github.com/zdgeier/jam/gen/jampb"
	"github.com/zdgeier/jam/pkg/fastcdc"
	"github.com/zdgeier/jam/pkg/jamfilelist"
	"github.com/zdgeier/jam/pkg/jamgrpc/serverauth"
	"github.com/zdgeier/jam/pkg/jamstores/file"
	"github.com/zdgeier/jam/pkg/jamstores/merger"
	"github.com/zdgeier/jam/pkg/jamstores/stores"
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

	db, err := stores.GetProjectDB(in.GetOwnerUsername(), in.GetProjectId())
	if err != nil {
		return nil, err
	}
	defer db.Close()

	maxCommitId, err := stores.MaxCommitId(db)
	if err != nil {
		return nil, err
	}

	workspaceId, err := stores.AddWorkspace(db, in.GetWorkspaceName(), maxCommitId)
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
	db, err := stores.GetProjectDB(in.GetOwnerUsername(), in.GetProjectId())
	if err != nil {
		return nil, err
	}
	defer db.Close()

	workspaceName, err := stores.GetWorkspaceNameById(db, in.GetWorkspaceId())
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
	db, err := stores.GetProjectDB(in.GetOwnerUsername(), in.GetProjectId())
	if err != nil {
		return nil, err
	}
	defer db.Close()

	workspaceId, err := stores.GetWorkspaceIdByName(db, in.GetWorkspaceName())
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
	db, err := stores.GetProjectDB(in.GetOwnerUsername(), in.GetProjectId())
	if err != nil {
		return nil, err
	}
	defer db.Close()

	changeId, err := stores.MaxWorkspaceChangeId(db, in.GetOwnerUsername(), in.GetProjectId(), in.GetWorkspaceId())
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

	db, err := stores.GetProjectDB(in.GetOwnerUsername(), in.GetProjectId())
	if err != nil {
		return nil, err
	}
	defer db.Close()

	workspaces, err := stores.ListWorkspaces(db)
	if err != nil {
		return nil, err
	}

	baseCommitIds := make(map[uint64]uint64)
	for _, workspaceId := range workspaces {
		baseCommitId, err := stores.GetWorkspaceBaseCommitId(db, workspaceId)
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
	db, err := stores.GetProjectDB(in.GetOwnerUsername(), in.GetProjectId())
	if err != nil {
		return nil, err
	}
	defer db.Close()

	newChangeId, err := stores.AddChange(db, in.GetOwnerUsername(), in.GetProjectId(), in.GetWorkspaceId())
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
		projectDB, workspaceDB           *sql.DB
		projectTx, workspaceTx           *sql.Tx
		projectStmt, workspaceStmt       *sql.Stmt
	)
	_, err := serverauth.ParseIdFromCtx(srv.Context())
	if err != nil {
		return err
	}

	for {
		in, err := srv.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if projectOwner == "" {
			// First operation
			projectOwner, projectId, workspaceId, changeId, err = s.db.GetOperationStreamTokenInfo(in.GetOperationToken())
			if err != nil {
				panic(err)
			}

			projectDB, err = stores.GetProjectDB(projectOwner, projectId)
			if err != nil {
				panic(err)
			}
			defer projectDB.Close()

			projectTx, err = projectDB.Begin()
			if err != nil {
				panic(err)
			}

			projectStmt, err = projectTx.Prepare("INSERT INTO workspace_chunk_hashes (workspace_id, change_id, path_hash, hash, offset, length) VALUES (?, ?, ?, ?, ?, ?)")
			if err != nil {
				panic(err)
			}
			defer projectStmt.Close()

			workspaceDB, err = stores.GetWorkspaceDataDB(projectOwner, projectId, workspaceId)
			if err != nil {
				panic(err)
			}
			defer workspaceDB.Close()

			workspaceTx, err = workspaceDB.Begin()
			if err != nil {
				panic(err)
			}

			workspaceStmt, err = workspaceTx.Prepare("INSERT INTO hashes (path_hash, hash, data) VALUES(?, ?, ?)")
			if err != nil {
				panic(err)
			}
			defer workspaceStmt.Close()
		}

		for _, op := range in.Operations {
			if op.Chunk != nil {
				hashString := strconv.FormatUint(op.Chunk.Hash, 10)
				_, err = workspaceStmt.Exec(op.PathHash, hashString, op.Chunk.Data)
				if err != nil {
					in.Operations = append(in.Operations, op)
					continue
				}

				_, err = projectStmt.Exec(int64(workspaceId), int64(changeId), op.PathHash, hashString, int64(op.Chunk.Offset), int64(op.Chunk.Length))
				if err != nil {
					in.Operations = append(in.Operations, op)
					continue
				}
			}
		}
	}

	err = projectTx.Commit()
	if err != nil {
		panic(err)
	}

	err = workspaceTx.Commit()
	if err != nil {
		panic(err)
	}

	return srv.SendAndClose(&jampb.WriteWorkspaceOperationsResponse{})
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

	workspaceConn, err := stores.GetWorkspaceDataDB(in.OwnerUsername, in.ProjectId, in.WorkspaceId)
	if err != nil {
		panic(err)
	}
	defer workspaceConn.Close()

	commitConn, err := stores.GetCommitDataDB(in.OwnerUsername, in.ProjectId)
	if err != nil {
		panic(err)
	}
	defer commitConn.Close()

	hashLists := make([]*jampb.HashList, 0, len(in.PathHashes))
	for _, pathHash := range in.PathHashes {
		// optimization: check if files exists, if not, return empty hash list

		hashList, err := stores.GetChunkHashes(workspaceConn, pathHash)
		if err != nil {
			panic(err)
		}

		committedHashList, err := stores.GetChunkHashes(commitConn, pathHash)
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

	db, err := stores.GetProjectDB(in.GetOwnerUsername(), in.GetProjectId())
	if err != nil {
		return err
	}
	defer db.Close()

	query, err := db.Prepare(`SELECT hash, offset, length FROM workspace_chunk_hashes
			WHERE change_id = (
				SELECT MAX(change_id) FROM workspace_chunk_hashes
				WHERE path_hash = ? AND workspace_id = ? AND change_id <= ?
			) AND path_hash = ? AND workspace_id = ?;`)
	if err != nil {
		return err
	}
	defer query.Close()

	chunkHashes, err := stores.ListWorkspaceChunkHashes(db, query, in.WorkspaceId, in.ChangeId, in.PathHash)
	if err != nil {
		return err
	}

	hashes := make(map[uint64][]byte)
	for _, chunkHash := range chunkHashes {
		hashes[chunkHash.Hash] = nil
	}

	workspaceConn, err := stores.GetWorkspaceDataDB(in.OwnerUsername, in.ProjectId, in.WorkspaceId)
	if err != nil {
		return err
	}
	defer workspaceConn.Close()

	workspaceDataStmt, err := workspaceConn.Prepare("SELECT data FROM hashes WHERE path_hash = ? AND hash = ?")
	if err != nil {
		return err
	}
	defer workspaceDataStmt.Close()

	commitConn, err := stores.GetCommitDataDB(in.OwnerUsername, in.ProjectId)
	if err != nil {
		return err
	}
	defer commitConn.Close()

	commitDataStmt, err := commitConn.Prepare("SELECT data FROM hashes WHERE path_hash = ? AND hash = ?")
	if err != nil {
		return err
	}
	defer commitDataStmt.Close()

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
			data, err := stores.Read(workspaceDataStmt, in.PathHash, chunk.Hash)
			if err != nil || len(data) == 0 {
				data, err = stores.Read(commitDataStmt, in.PathHash, chunk.Hash)
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

	db, err := stores.GetProjectDB(in.GetOwnerUsername(), in.GetProjectId())
	if err != nil {
		return nil, err
	}
	defer db.Close()

	err = stores.DeleteWorkspace(db, in.GetWorkspaceId())
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	return &jampb.DeleteWorkspaceResponse{}, nil
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

	db, err := stores.GetProjectDB(in.GetOwnerUsername(), in.GetProjectId())
	if err != nil {
		return nil, err
	}
	defer db.Close()

	changedWorkspacePathHashes, err := stores.ListWorkspaceChangedPathHashes(db, in.GetWorkspaceId())
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	workspaceBaseCommitId, err := stores.GetWorkspaceBaseCommitId(db, in.GetWorkspaceId())
	if err != nil {
		return nil, err
	}

	maxCommitId, err := stores.MaxCommitId(db)
	if err != nil {
		return nil, err
	}

	if workspaceBaseCommitId == maxCommitId {
		return nil, errors.New("already up-to-date")
	}

	changedCommitPathHashes, err := stores.ListChangedPathHashesFromCommit(db, workspaceBaseCommitId)
	if err != nil {
		return nil, err
	}

	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	bothChangedPathHashes := make(map[string]interface{})
	for workspacePathHash := range changedWorkspacePathHashes {
		if _, ok := changedCommitPathHashes[workspacePathHash]; ok {
			if string(workspacePathHash) == ".jamfilelist" {
				continue
			}
			bothChangedPathHashes[string(workspacePathHash)] = nil
		}
	}

	maxChangeId, err := stores.MaxWorkspaceChangeId(db, in.GetOwnerUsername(), in.GetProjectId(), in.GetWorkspaceId())
	if err != nil {
		return nil, err
	}

	query, err := db.Prepare(`SELECT hash, offset, length FROM workspace_chunk_hashes
			WHERE change_id = (
				SELECT MAX(change_id) FROM workspace_chunk_hashes
				WHERE path_hash = ? AND workspace_id = ? AND change_id <= ?
			) AND path_hash = ? AND workspace_id = ?;`)
	if err != nil {
		return nil, err
	}
	defer query.Close()

	workspaceConn, err := stores.GetWorkspaceDataDB(in.GetOwnerUsername(), in.GetProjectId(), in.GetWorkspaceId())
	if err != nil {
		return nil, err
	}
	defer workspaceConn.Close()

	workspaceDataStmt, err := workspaceConn.Prepare("SELECT data FROM hashes WHERE path_hash = ? AND hash = ?")
	if err != nil {
		return nil, err
	}
	defer workspaceDataStmt.Close()

	workspaceTx, err := workspaceConn.Begin()
	if err != nil {
		return nil, err
	}

	workspaceInsertStmt, err := workspaceTx.Prepare("INSERT INTO hashes (path_hash, hash, data) VALUES(?, ?, ?)")
	if err != nil {
		return nil, err
	}
	defer workspaceInsertStmt.Close()

	stmt, err := db.Prepare(`SELECT hash, offset, length FROM workspace_chunk_hashes
			WHERE change_id = (
				SELECT MAX(change_id) FROM workspace_chunk_hashes
				WHERE path_hash = ? AND workspace_id = ? AND change_id <= ?
			) AND path_hash = ? AND workspace_id = ?;`)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	commitConn, err := stores.GetCommitDataDB(in.GetOwnerUsername(), in.GetProjectId())
	if err != nil {
		return nil, err
	}
	defer commitConn.Close()

	commitDataStmt, err := commitConn.Prepare("SELECT data FROM hashes WHERE path_hash = ? AND hash = ?")
	if err != nil {
		return nil, err
	}
	defer commitDataStmt.Close()

	fileListBuffer := bytes.NewBuffer(nil)
	{
		chunkHashes, err := stores.ListWorkspaceChunkHashes(db, stmt, in.WorkspaceId, maxChangeId, file.PathToHash(".jamfilelist"))
		if err != nil {
			return nil, err
		}
		offset := uint64(0)
		for _, chunkHash := range chunkHashes {
			data, err := stores.Read(workspaceDataStmt, file.PathToHash(".jamfilelist"), chunkHash.Hash)
			if err != nil || len(data) == 0 {
				data, err = stores.Read(commitDataStmt, file.PathToHash(".jamfilelist"), chunkHash.Hash)
				if err != nil {
					return nil, err
				}
			}
			n, err := fileListBuffer.Write(data)
			if err != nil {
				return nil, err
			}
			if n != len(data) || n != int(chunkHash.Length) {
				return nil, errors.New("failed to write all data")
			} else if offset != chunkHash.Offset {
				return nil, errors.New("invalid offset while writing workspace data")
			}
			offset += chunkHash.Length
		}
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

	newChangeId, err := stores.AddChange(db, in.GetOwnerUsername(), in.GetProjectId(), in.WorkspaceId)
	if err != nil {
		return nil, err
	}

	conflicts := make([]string, 0)
	newChunker, err := fastcdc.NewJamChunker(fastcdc.DefaultOpts)
	if err != nil {
		return nil, err
	}

	// Merges all the paths that have changed in both the workspace and the mainline
	for pathHashString := range bothChangedPathHashes {
		pathHash := []byte(pathHashString)

		oldCommittedFile := bytes.NewBuffer([]byte{})
		{
			chunkHashes, err := stores.ListCommitChunkHashes(db, workspaceBaseCommitId, pathHash)
			if err != nil {
				return nil, err
			}

			offset := uint64(0)

			for _, chunkHash := range chunkHashes {
				data, err := stores.Read(commitDataStmt, pathHash, chunkHash.Hash)
				if err != nil {
					return nil, err
				}
				n, err := oldCommittedFile.Write(data)
				if err != nil {
					return nil, err
				}
				if n != len(data) || n != int(chunkHash.Length) {
					return nil, errors.New("failed to write all data")
				} else if offset != chunkHash.Offset {
					return nil, errors.New("invalid offset while writing committed data")
				}
				offset += chunkHash.Length
			}
		}

		currentCommittedFile := bytes.NewBuffer([]byte{})
		{
			chunkHashes, err := stores.ListCommitChunkHashes(db, maxCommitId, pathHash)
			if err != nil {
				return nil, err
			}

			offset := uint64(0)

			for _, chunkHash := range chunkHashes {
				data, err := stores.Read(commitDataStmt, pathHash, chunkHash.Hash)
				if err != nil {
					return nil, err
				}
				n, err := currentCommittedFile.Write(data)
				if err != nil {
					return nil, err
				}
				if n != len(data) || n != int(chunkHash.Length) {
					return nil, errors.New("failed to write all data")
				} else if offset != chunkHash.Offset {
					return nil, errors.New("invalid offset while writing committed data")
				}
				offset += chunkHash.Length
			}
		}

		workspaceFile := bytes.NewBuffer([]byte{})
		{
			chunkHashes, err := stores.ListWorkspaceChunkHashes(db, stmt, in.WorkspaceId, maxChangeId, pathHash)
			if err != nil {
				return nil, err
			}
			offset := uint64(0)
			for _, chunkHash := range chunkHashes {
				data, err := stores.Read(workspaceDataStmt, pathHash, chunkHash.Hash)
				if err != nil || len(data) == 0 {
					data, err = stores.Read(commitDataStmt, pathHash, chunkHash.Hash)
					if err != nil {
						return nil, err
					}
				}
				n, err := workspaceFile.Write(data)
				if err != nil {
					return nil, err
				}
				if n != len(data) || n != int(chunkHash.Length) {
					return nil, errors.New("failed to write all data")
				} else if offset != chunkHash.Offset {
					return nil, errors.New("invalid offset while writing workspace data")
				}
				offset += chunkHash.Length
			}
		}

		var specificFilePath string
		var specificFileMetadata *jampb.File
		for _, pathFile := range fileMetadata.Files {
			if bytes.Equal(file.PathToHash(pathFile.Path), pathHash) {
				specificFilePath = pathFile.Path
				specificFileMetadata = pathFile.File
				break
			}
		}

		mergedFile, err := merger.Merge(in.GetOwnerUsername(), in.GetProjectId(), specificFilePath, oldCommittedFile, workspaceFile, currentCommittedFile)
		if err != nil {
			return nil, err
		}

		mergedData, _ := io.ReadAll(mergedFile)
		b := xxh3.Hash128(mergedData).Bytes()
		mergedFile.Seek(0, 0)

		conflicts = append(conflicts, specificFilePath)
		specificFileMetadata.Hash = b[:]

		newChunker.SetChunkerReader(mergedFile)

		newChunks := make([]*jampb.Chunk, 0)
		err = newChunker.CreateDelta(nil, func(chunk *jampb.Chunk) error {
			newChunks = append(newChunks, chunk)
			return nil
		})
		if err != nil {
			return nil, err
		}

		newChunkHashes := make([]*jampb.ChunkHash, 0)
		for _, newChunk := range newChunks {
			err := stores.Write(workspaceInsertStmt, pathHash, newChunk.Hash, newChunk.Data)
			if err != nil {
				return nil, err
			}
			newChunkHashes = append(newChunkHashes, &jampb.ChunkHash{
				Hash:   newChunk.Hash,
				Offset: newChunk.Offset,
				Length: newChunk.Length,
			})
		}

		err = stores.InsertWorkspaceChunkHashes(db, in.WorkspaceId, newChangeId, pathHash, newChunkHashes)
		if err != nil {
			return nil, err
		}
	}

	// Update file list with new hashes
	{
		sort.Sort(jamfilelist.PathFileList(fileMetadata.Files))
		fileMetadataBytes, err := proto.Marshal(fileMetadata)
		if err != nil {
			return nil, err
		}
		newContent := bytes.NewReader(fileMetadataBytes)
		newChunker.SetChunkerReader(newContent)
		newChunks := make([]*jampb.Chunk, 0)
		err = newChunker.CreateDelta(nil, func(chunk *jampb.Chunk) error {
			newChunks = append(newChunks, chunk)
			return nil
		})
		if err != nil {
			return nil, err
		}

		newChunkHashes := make([]*jampb.ChunkHash, 0)
		for _, newChunk := range newChunks {
			err := stores.Write(workspaceDataStmt, file.PathToHash(".jamfilelist"), newChunk.Hash, newChunk.Data)
			if err != nil {
				return nil, err
			}
			newChunkHashes = append(newChunkHashes, &jampb.ChunkHash{
				Hash:   newChunk.Hash,
				Offset: newChunk.Offset,
				Length: newChunk.Length,
			})
		}

		err = stores.InsertWorkspaceChunkHashes(db, in.WorkspaceId, newChangeId, file.PathToHash(".jamfilelist"), newChunkHashes)
		if err != nil {
			return nil, err
		}
	}

	err = workspaceTx.Commit()
	if err != nil {
		return nil, err
	}

	err = stores.UpdateWorkspaceBaseCommit(db, in.GetWorkspaceId(), maxCommitId)
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

	db, err := stores.GetProjectDB(in.GetOwnerUsername(), in.GetProjectId())
	if err != nil {
		return nil, err
	}
	defer db.Close()
	newChangeId, err := stores.AddChange(db, in.GetOwnerUsername(), in.GetProjectId(), in.GetWorkspaceId())
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

	projectDb, err := stores.GetProjectDB(in.GetOwnerUsername(), in.GetProjectId())
	if err != nil {
		return nil, err
	}

	baseCommitId, err := stores.GetWorkspaceBaseCommitId(projectDb, in.GetWorkspaceId())
	if err != nil {
		return nil, err
	}

	prevCommitId, err := stores.MaxCommitId(projectDb)
	if err != nil {
		return nil, err
	}

	if baseCommitId != prevCommitId {
		return nil, errors.New("workspace is not up to date with latest commit")
	}

	changedPathHashes, err := stores.ListWorkspaceChangedPathHashes(projectDb, in.GetWorkspaceId())
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if len(changedPathHashes) == 0 {
		return &jampb.MergeWorkspaceResponse{CommitId: prevCommitId}, nil
	}

	maxChangeId, err := stores.MaxWorkspaceChangeId(projectDb, in.GetOwnerUsername(), in.GetProjectId(), in.GetWorkspaceId())
	if err != nil {
		return nil, err
	}

	newCommitId, err := stores.AddCommit(projectDb, in.GetMergeMessage())
	if err != nil {
		return nil, err
	}

	dbTx, err := projectDb.Begin()
	if err != nil {
		panic(err)
	}

	insertStmt, err := dbTx.Prepare("INSERT INTO commit_chunk_hashes (commit_id, path_hash, hash, offset, length) VALUES (?, ?, ?, ?, ?)")
	if err != nil {
		panic(err)
	}
	defer insertStmt.Close()

	query, err := projectDb.Prepare(`SELECT hash, offset, length FROM workspace_chunk_hashes
			WHERE change_id = (
				SELECT MAX(change_id) FROM workspace_chunk_hashes
				WHERE path_hash = ? AND workspace_id = ? AND change_id <= ?
			) AND path_hash = ? AND workspace_id = ?;`)
	if err != nil {
		return nil, err
	}
	defer query.Close()

	workspaceDatastoreConn, err := stores.GetWorkspaceDataDB(in.OwnerUsername, in.ProjectId, in.WorkspaceId)
	if err != nil {
		panic(err)
	}
	defer workspaceDatastoreConn.Close()

	workspaceDatastoreTx, err := workspaceDatastoreConn.Begin()
	if err != nil {
		panic(err)
	}

	workspaceDatastoreStmt, err := workspaceDatastoreTx.Prepare("SELECT data FROM hashes WHERE path_hash = ? AND hash = ?")
	if err != nil {
		return nil, err
	}
	defer workspaceDatastoreStmt.Close()

	commitDatastoreConn, err := stores.GetCommitDataDB(in.OwnerUsername, in.ProjectId)
	if err != nil {
		panic(err)
	}
	defer commitDatastoreConn.Close()

	commitDatastoreTx, err := commitDatastoreConn.Begin()
	if err != nil {
		return nil, err
	}

	commitDatastoreStmt, err := commitDatastoreTx.Prepare("INSERT INTO hashes (path_hash, hash, data) VALUES(?, ?, ?)")
	if err != nil {
		return nil, err
	}
	defer commitDatastoreStmt.Close()

	for pathHashString := range changedPathHashes {
		pathHash := []byte(pathHashString)

		chunkHashes, err := stores.ListWorkspaceChunkHashes(projectDb, query, in.GetWorkspaceId(), maxChangeId, []byte(pathHash))
		if err != nil {
			panic(err)
		}

		for _, chunkHash := range chunkHashes {
			buf, err := stores.Read(workspaceDatastoreStmt, pathHash, chunkHash.Hash)
			if err != nil {
				panic(err)
			}
			err = stores.Write(commitDatastoreStmt, pathHash, chunkHash.Hash, buf)
			if err != nil {
				panic(err)
			}
			hashString := strconv.FormatUint(chunkHash.Hash, 10)
			_, err = insertStmt.Exec(int64(newCommitId), pathHash, hashString, int64(chunkHash.Offset), int64(chunkHash.Length))
			if err != nil {
				panic(err)
			}
		}
	}

	err = workspaceDatastoreTx.Commit()
	if err != nil {
		return nil, err
	}

	err = commitDatastoreTx.Commit()
	if err != nil {
		return nil, err
	}

	err = dbTx.Commit()
	if err != nil {
		return nil, err
	}

	err = stores.UpdateWorkspaceBaseCommit(projectDb, in.GetWorkspaceId(), newCommitId)
	if err != nil {
		return nil, err
	}

	return &jampb.MergeWorkspaceResponse{
		CommitId: newCommitId,
	}, nil
}
