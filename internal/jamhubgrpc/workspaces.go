package jamhubgrpc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/zdgeier/jamhub/gen/pb"
	"github.com/zdgeier/jamhub/internal/fastcdc"
	"github.com/zdgeier/jamhub/internal/jamhub/file"
	"github.com/zdgeier/jamhub/internal/jamhubgrpc/serverauth"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s JamHub) CreateWorkspace(ctx context.Context, in *pb.CreateWorkspaceRequest) (*pb.CreateWorkspaceResponse, error) {
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

	maxCommitId, err := s.oplocstorecommit.MaxCommitId(in.GetOwnerUsername(), in.GetProjectId())
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	workspaceId, err := s.changestore.AddWorkspace(in.GetOwnerUsername(), in.GetProjectId(), in.GetWorkspaceName(), maxCommitId)
	if err != nil {
		return nil, err
	}

	return &pb.CreateWorkspaceResponse{
		WorkspaceId: workspaceId,
	}, nil
}

func (s JamHub) GetWorkspaceName(ctx context.Context, in *pb.GetWorkspaceNameRequest) (*pb.GetWorkspaceNameResponse, error) {
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

	workspaceName, err := s.changestore.GetWorkspaceNameById(in.GetOwnerUsername(), in.GetProjectId(), in.GetWorkspaceId())
	if err != nil {
		return nil, err
	}

	return &pb.GetWorkspaceNameResponse{
		WorkspaceName: workspaceName,
	}, nil
}

func (s JamHub) GetWorkspaceId(ctx context.Context, in *pb.GetWorkspaceIdRequest) (*pb.GetWorkspaceIdResponse, error) {
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

	workspaceId, err := s.changestore.GetWorkspaceIdByName(in.GetOwnerUsername(), in.GetProjectId(), in.GetWorkspaceName())
	if err != nil {
		return nil, err
	}

	return &pb.GetWorkspaceIdResponse{
		WorkspaceId: workspaceId,
	}, nil
}

func (s JamHub) GetWorkspaceCurrentChange(ctx context.Context, in *pb.GetWorkspaceCurrentChangeRequest) (*pb.GetWorkspaceCurrentChangeResponse, error) {
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

	changeId, err := s.oplocstoreworkspace.MaxChangeId(in.GetOwnerUsername(), in.GetProjectId(), in.GetWorkspaceId())
	if err != nil {
		return nil, err
	}

	return &pb.GetWorkspaceCurrentChangeResponse{
		ChangeId: changeId,
	}, nil
}

func (s JamHub) ListWorkspaces(ctx context.Context, in *pb.ListWorkspacesRequest) (*pb.ListWorkspacesResponse, error) {
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

	workspaces, err := s.changestore.ListWorkspaces(in.GetOwnerUsername(), in.GetProjectId())
	if err != nil {
		return nil, err
	}

	return &pb.ListWorkspacesResponse{
		Workspaces: workspaces,
	}, nil
}

func (s JamHub) WriteWorkspaceOperationsStream(srv pb.JamHub_WriteWorkspaceOperationsStreamServer) error {
	userId, err := serverauth.ParseIdFromCtx(srv.Context())
	if err != nil {
		return err
	}

	currentUsername, err := s.db.GetUsername(userId)
	if err != nil {
		return err
	}

	var projectOwner string
	var projectId, workspaceId, changeId, operationProject uint64
	pathHashToOpLocs := make(map[string][]*pb.WorkspaceOperationLocations_OperationLocation, 0)
	for {
		in, err := srv.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		projectId = in.GetProjectId()
		workspaceId = in.GetWorkspaceId()
		changeId = in.GetChangeId()
		if operationProject == 0 {
			accessible, err := s.ProjectIdAccessible(in.GetOwnerUsername(), in.GetProjectId(), currentUsername)
			if err != nil {
				return err
			}

			if !accessible {
				return errors.New("not an owner or collaborator of this project")
			}
			projectOwner = in.GetOwnerUsername()
			operationProject = projectId
		}

		if operationProject != projectId {
			return status.Errorf(codes.Unauthenticated, "unauthorized")
		}

		pathHash := in.GetPathHash()

		var chunkHash *pb.ChunkHash
		var workspaceOffset, workspaceLength, commitOffset, commitLength uint64
		if in.GetOp().GetType() == pb.Operation_OpData {
			workspaceOffset, workspaceLength, err = s.opdatastoreworkspace.Write(projectOwner, projectId, workspaceId, pathHash, in.GetOp())
			if err != nil {
				return err
			}
			chunkHash = &pb.ChunkHash{
				Offset: in.GetOp().GetChunk().GetOffset(),
				Length: in.GetOp().GetChunk().GetLength(),
				Hash:   in.GetOp().GetChunk().GetHash(),
			}
		} else {
			chunkHash = &pb.ChunkHash{
				Offset: in.GetOp().GetChunkHash().GetOffset(),
				Length: in.GetOp().GetChunkHash().GetLength(),
				Hash:   in.GetOp().GetChunkHash().GetHash(),
			}
		}

		if in.GetOp().GetType() == pb.Operation_OpBlock {
			opLocs, err := s.oplocstoreworkspace.ListOperationLocations(projectOwner, projectId, workspaceId, changeId-1, pathHash)
			if err != nil {
				return err
			}
			for _, loc := range opLocs.GetOpLocs() {
				if loc.GetChunkHash().GetHash() == in.GetOp().GetChunkHash().GetHash() {
					workspaceOffset = loc.GetOffset()
					workspaceLength = loc.GetLength()
					break
				}
			}

			if workspaceOffset == 0 && workspaceLength == 0 {
				commitId, err := s.changestore.GetWorkspaceBaseCommitId(projectOwner, projectId, workspaceId)
				if err != nil {
					return err
				}

				commitOpLocs, err := s.oplocstorecommit.ListOperationLocations(projectOwner, projectId, commitId, pathHash)
				if err != nil {
					return err
				}
				for _, loc := range commitOpLocs.GetOpLocs() {
					if loc.GetChunkHash().GetHash() == in.GetOp().GetChunkHash().GetHash() {
						commitOffset = loc.GetOffset()
						commitLength = loc.GetLength()
						break
					}
				}

				if commitOffset == 0 && commitLength == 0 {
					log.Panic("Operation of type block but hash could not be found in workspace or commit")
				}
			}
		}

		operationLocation := &pb.WorkspaceOperationLocations_OperationLocation{
			Offset:       workspaceOffset,
			Length:       workspaceLength,
			CommitOffset: commitOffset,
			CommitLength: commitLength,
			ChunkHash:    chunkHash,
		}
		pathHashToOpLocs[string(pathHash)] = append(pathHashToOpLocs[string(pathHash)], operationLocation)
	}

	for pathHash, opLocs := range pathHashToOpLocs {
		err = s.oplocstoreworkspace.InsertOperationLocations(&pb.WorkspaceOperationLocations{
			ProjectId:     projectId,
			OwnerUsername: projectOwner,
			WorkspaceId:   workspaceId,
			ChangeId:      changeId,
			PathHash:      []byte(pathHash),
			OpLocs:        opLocs,
		})
		if err != nil {
			return err
		}
	}

	return srv.SendAndClose(&pb.WriteOperationStreamResponse{})
}

func (s JamHub) ReadWorkspaceChunkHashes(ctx context.Context, in *pb.ReadWorkspaceChunkHashesRequest) (*pb.ReadWorkspaceChunkHashesResponse, error) {
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

	targetBuffer, err := s.regenWorkspaceFile(in.GetOwnerUsername(), in.GetProjectId(), in.GetWorkspaceId(), in.GetChangeId(), in.GetPathHash())
	if err != nil {
		return nil, err
	}

	targetChunker, err := fastcdc.NewJamChunker(targetBuffer)
	if err != nil {
		return nil, err
	}
	sig := make([]*pb.ChunkHash, 0)
	err = targetChunker.CreateSignature(func(ch *pb.ChunkHash) error {
		sig = append(sig, ch)
		return nil
	})
	return &pb.ReadWorkspaceChunkHashesResponse{
		ChunkHashes: sig,
	}, err
}

func (s JamHub) regenWorkspaceFile(ownerUsername string, projectId, workspaceId, changeId uint64, pathHash []byte) (*bytes.Reader, error) {
	commitId, err := s.changestore.GetWorkspaceBaseCommitId(ownerUsername, projectId, workspaceId)
	if err != nil {
		panic(err)
	}

	committedFileReader, err := s.regenCommittedFile(ownerUsername, projectId, commitId, pathHash)
	if err != nil {
		panic(err)
	}

	var operationLocations *pb.WorkspaceOperationLocations
	for i := int(changeId); i >= 0 && operationLocations == nil; i-- {
		operationLocations, err = s.oplocstoreworkspace.ListOperationLocations(ownerUsername, projectId, workspaceId, uint64(i), pathHash)
		if err != nil {
			panic(err)
		}
	}
	if operationLocations == nil {
		return committedFileReader, nil
	}

	ops := make(chan *pb.Operation)
	go func() {
		for _, loc := range operationLocations.GetOpLocs() {
			if loc.GetCommitLength() != 0 {
				op, err := s.opdatastorecommit.Read(ownerUsername, projectId, pathHash, loc.GetCommitOffset(), loc.GetCommitLength())
				if err != nil {
					log.Panic(err)
				}
				ops <- op
			} else {
				op, err := s.opdatastoreworkspace.Read(ownerUsername, projectId, workspaceId, pathHash, loc.GetOffset(), loc.GetLength())
				if err != nil {
					log.Panic(err)
				}
				ops <- op
			}
		}
		close(ops)
	}()
	result := new(bytes.Buffer)
	chunker, err := fastcdc.NewJamChunker(committedFileReader)
	if err != nil {
		log.Panic(err)
	}
	err = chunker.ApplyDelta(result, committedFileReader, ops)
	if err != nil {
		log.Panic(err)
	}

	return bytes.NewReader(result.Bytes()), nil
}

func (s JamHub) ReadWorkspaceFile(in *pb.ReadWorkspaceFileRequest, srv pb.JamHub_ReadWorkspaceFileServer) error {
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

	changeId := in.GetChangeId()
	if changeId == 0 {
		maxChangeId, err := s.oplocstoreworkspace.MaxChangeId(in.GetOwnerUsername(), in.GetProjectId(), in.GetWorkspaceId())
		if err != nil {
			return err
		}
		changeId = maxChangeId
	}

	sourceBuffer, err := s.regenWorkspaceFile(in.GetOwnerUsername(), in.GetProjectId(), in.GetWorkspaceId(), changeId, in.GetPathHash())
	if err != nil {
		return err
	}

	sourceChunker, err := fastcdc.NewJamChunker(sourceBuffer)
	if err != nil {
		return err
	}

	opsOut := make(chan *pb.Operation)
	tot := 0
	go func() {
		var blockCt, dataCt, bytes int
		defer close(opsOut)
		err := sourceChunker.CreateDelta(in.GetChunkHashes(), func(op *pb.Operation) error {
			tot += int(op.Chunk.GetLength()) + int(op.ChunkHash.GetLength())
			switch op.Type {
			case pb.Operation_OpBlock:
				blockCt++
			case pb.Operation_OpData:
				b := make([]byte, len(op.Chunk.Data))
				copy(b, op.Chunk.Data)
				op.Chunk.Data = b
				dataCt++
				bytes += len(op.Chunk.Data)
			}
			opsOut <- op
			return nil
		})
		if err != nil {
			panic(err)
		}
	}()

	for op := range opsOut {
		err = srv.Send(&pb.WorkspaceFileOperation{
			WorkspaceId:   in.WorkspaceId,
			OwnerUsername: in.GetOwnerUsername(),
			ProjectId:     in.GetProjectId(),
			PathHash:      in.GetPathHash(),
			Op:            op,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (s JamHub) DeleteWorkspace(ctx context.Context, in *pb.DeleteWorkspaceRequest) (*pb.DeleteWorkspaceResponse, error) {
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

	err = s.opdatastoreworkspace.DeleteWorkspace(in.GetOwnerUsername(), in.GetProjectId(), in.GetWorkspaceId())
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	err = s.oplocstoreworkspace.DeleteWorkspace(in.GetOwnerUsername(), in.GetProjectId(), in.GetWorkspaceId())
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	err = s.changestore.DeleteWorkspace(in.GetOwnerUsername(), in.GetProjectId(), in.GetWorkspaceId())
	if err != nil {
		return nil, err
	}

	return &pb.DeleteWorkspaceResponse{}, nil
}

func (s JamHub) UpdateWorkspace(ctx context.Context, in *pb.UpdateWorkspaceRequest) (*pb.UpdateWorkspaceResponse, error) {
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

	// prevCommitId, err := s.oplocstorecommit.MaxCommitId(in.GetOwnerUsername(), in.GetProjectId())
	// if err != nil {
	// 	return nil, err
	// }

	// Regen every file that has been changed in workspace
	changedWorkspacePathHashes, err := s.opdatastoreworkspace.GetChangedPathHashes(in.GetOwnerUsername(), in.GetProjectId(), in.GetWorkspaceId())
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	workspaceBaseCommitId, err := s.changestore.GetWorkspaceBaseCommitId(in.GetOwnerUsername(), in.GetProjectId(), in.GetWorkspaceId())
	if err != nil {
		panic(err)
	}

	maxCommitId, err := s.oplocstorecommit.MaxCommitId(in.GetOwnerUsername(), in.GetProjectId())
	if err != nil {
		return nil, err
	}

	changedCommitPathHashes, err := s.oplocstorecommit.GetChangedPathHashes(in.GetOwnerUsername(), in.GetProjectId(), workspaceBaseCommitId, maxCommitId)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	// Check for conflicts
	bothChangedPathHashes := make(map[string]interface{})
	for workspacePathHash := range changedWorkspacePathHashes {
		if _, ok := changedCommitPathHashes[workspacePathHash]; ok {
			bothChangedPathHashes[string(workspacePathHash)] = nil
		}
	}

	// if len(conflicts) != 0 {
	// 	return nil, fmt.Errorf("conflict with mainline not supported yet: %d conflicts", len(conflicts))
	// }

	for conflict := range bothChangedPathHashes {
		fmt.Println("BOTH CHANGED", conflict)
	}

	maxChangeId, err := s.oplocstoreworkspace.MaxChangeId(in.GetOwnerUsername(), in.GetProjectId(), in.GetWorkspaceId())
	if err != nil {
		return nil, err
	}

	pathHashes := make(chan []byte)
	results := make(chan error)

	makeDiff := func() {
		for pathHash := range pathHashes {
			if bytes.Equal(pathHash, file.PathToHash(".jamfilelist")) {
				// Ignore file list for now since it's not able to be merged
				results <- nil
				continue
			}

			fmt.Println("EQUAL?", bytes.Equal(pathHash, file.PathToHash(".jamfilelist")), pathHash, file.PathToHash(".jamfilelist"))
			committedBaseFileReader, err := s.regenCommittedFile(in.GetOwnerUsername(), in.GetProjectId(), workspaceBaseCommitId, pathHash)
			if err != nil {
				panic(err)
			}

			committedCurrentFileReader, err := s.regenCommittedFile(in.GetOwnerUsername(), in.GetProjectId(), maxCommitId, pathHash)
			if err != nil {
				panic(err)
			}

			workspaceCurrentFileReader, err := s.regenWorkspaceFile(in.GetOwnerUsername(), in.GetProjectId(), in.GetWorkspaceId(), maxChangeId, pathHash)
			if err != nil {
				panic(err)
			}

			mergedFile, err := s.mergestore.Merge(in.GetOwnerUsername(), in.GetProjectId(), pathHash, committedBaseFileReader, committedCurrentFileReader, workspaceCurrentFileReader)
			if err != nil {
				panic(err)
			}

			out, _ := io.ReadAll(mergedFile)
			fmt.Println("MERGED", string(out))
			mergedFile.Seek(0, 0)

			sourceChunker, err := fastcdc.NewJamChunker(mergedFile)
			if err != nil {
				panic(err)
			}

			workspaceOperationLocations, err := s.ReadWorkspaceChunkHashes(ctx, &pb.ReadWorkspaceChunkHashesRequest{
				ProjectId:     in.GetProjectId(),
				OwnerUsername: in.GetOwnerUsername(),
				WorkspaceId:   in.WorkspaceId,
				ChangeId:      maxChangeId,
				PathHash:      pathHash,
			})
			if err != nil {
				panic(err)
			}

			opsOut := make(chan *pb.Operation)
			go func() {
				defer close(opsOut)
				err := sourceChunker.CreateDelta(workspaceOperationLocations.GetChunkHashes(), func(op *pb.Operation) error {
					switch op.Type {
					case pb.Operation_OpData:
						b := make([]byte, len(op.Chunk.Data))
						copy(b, op.Chunk.Data)
						op.Chunk.Data = b
					}
					opsOut <- op
					return nil
				})
				if err != nil {
					panic(err)
				}
			}()

			pathHashToOpLocs := make(map[string][]*pb.WorkspaceOperationLocations_OperationLocation, 0)
			for op := range opsOut {
				offset, length, err := s.opdatastoreworkspace.Write(in.GetOwnerUsername(), in.GetProjectId(), in.GetWorkspaceId(), pathHash, op)
				if err != nil {
					panic(err)
				}

				var chunkHash *pb.ChunkHash
				if op.GetType() == pb.Operation_OpData {
					chunkHash = &pb.ChunkHash{
						Offset: op.GetChunk().GetOffset(),
						Length: op.GetChunk().GetLength(),
						Hash:   op.GetChunk().GetHash(),
					}
				} else {
					chunkHash = &pb.ChunkHash{
						Offset: op.GetChunkHash().GetOffset(),
						Length: op.GetChunkHash().GetLength(),
						Hash:   op.GetChunkHash().GetHash(),
					}
				}

				if op.GetType() == pb.Operation_OpBlock {
					opLocs, err := s.oplocstoreworkspace.ListOperationLocations(in.GetOwnerUsername(), in.GetProjectId(), in.GetWorkspaceId(), maxChangeId, pathHash)
					if err != nil {
						panic(err)
					}
					found := false
					var reusedOffset, reusedLength uint64
					for _, loc := range opLocs.GetOpLocs() {
						if loc.GetChunkHash().GetHash() == op.GetChunkHash().GetHash() {
							found = true
							reusedOffset = loc.GetOffset()
							reusedLength = loc.GetLength()
							break
						}
					}
					if !found {
						log.Fatal("Operation of type block but hash could not be found")
					}
					offset = reusedOffset
					length = reusedLength
				}

				operationLocation := &pb.WorkspaceOperationLocations_OperationLocation{
					Offset:    offset,
					Length:    length,
					ChunkHash: chunkHash,
				}
				pathHashToOpLocs[string(pathHash)] = append(pathHashToOpLocs[string(pathHash)], operationLocation)
			}

			for pathHash, opLocs := range pathHashToOpLocs {
				err = s.oplocstoreworkspace.InsertOperationLocations(&pb.WorkspaceOperationLocations{
					ProjectId:     in.GetProjectId(),
					OwnerUsername: in.GetOwnerUsername(),
					WorkspaceId:   in.GetWorkspaceId(),
					ChangeId:      maxChangeId + 1,
					PathHash:      []byte(pathHash),
					OpLocs:        opLocs,
				})
				if err != nil {
					panic(err)
				}
			}

			results <- nil
		}
	}

	for i := 0; i < 128; i++ {
		go makeDiff()
	}

	go func() {
		for k := range bothChangedPathHashes {
			pathHashes <- []byte(k)
		}
	}()

	completed := 0
	for e := range results {
		if e != nil {
			panic(e)
		}
		completed += 1
		fmt.Println(completed)

		if completed == len(bothChangedPathHashes) {
			close(pathHashes)
			close(results)
		}
	}

	err = s.changestore.UpdateWorkspaceBaseCommit(in.GetOwnerUsername(), in.GetProjectId(), in.GetWorkspaceId(), maxCommitId)
	if err != nil {
		return nil, err
	}

	return &pb.UpdateWorkspaceResponse{}, nil

}

func (s JamHub) MergeWorkspace(ctx context.Context, in *pb.MergeWorkspaceRequest) (*pb.MergeWorkspaceResponse, error) {
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

	baseCommitId, err := s.changestore.GetWorkspaceBaseCommitId(in.GetOwnerUsername(), in.GetProjectId(), in.GetWorkspaceId())
	if err != nil {
		return nil, err
	}

	prevCommitId, err := s.oplocstorecommit.MaxCommitId(in.GetOwnerUsername(), in.GetProjectId())
	if err != nil {
		return nil, err
	}

	if baseCommitId != prevCommitId {
		return nil, errors.New("workspace is not up to date with latest commit")
	}

	// Regen every file that has been changed in workspace
	changedPathHashes, err := s.opdatastoreworkspace.GetChangedPathHashes(in.GetOwnerUsername(), in.GetProjectId(), in.GetWorkspaceId())
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if len(changedPathHashes) == 0 {
		// NO CHANGES
		return &pb.MergeWorkspaceResponse{CommitId: prevCommitId}, nil
	}

	maxChangeId, err := s.oplocstoreworkspace.MaxChangeId(in.GetOwnerUsername(), in.GetProjectId(), in.GetWorkspaceId())
	if err != nil {
		return nil, err
	}

	pathHashes := make(chan []byte)
	results := make(chan error)

	makeDiff := func() {
		for changedPathHash := range pathHashes {
			sourceReader, err := s.regenWorkspaceFile(in.GetOwnerUsername(), in.GetProjectId(), in.GetWorkspaceId(), maxChangeId, changedPathHash)
			if err != nil {
				panic(err)
			}

			commitOperationLocations, err := s.ReadCommitChunkHashes(ctx, &pb.ReadCommitChunkHashesRequest{
				ProjectId:     in.GetProjectId(),
				OwnerUsername: in.GetOwnerUsername(),
				CommitId:      prevCommitId,
				PathHash:      []byte(changedPathHash),
			})
			if err != nil {
				panic(err)
			}

			sourceChunker, err := fastcdc.NewJamChunker(sourceReader)
			if err != nil {
				panic(err)
			}

			opsOut := make(chan *pb.Operation)
			go func() {
				defer close(opsOut)
				err := sourceChunker.CreateDelta(commitOperationLocations.GetChunkHashes(), func(op *pb.Operation) error {
					switch op.Type {
					case pb.Operation_OpData:
						b := make([]byte, len(op.Chunk.Data))
						copy(b, op.Chunk.Data)
						op.Chunk.Data = b
					}
					opsOut <- op
					return nil
				})
				if err != nil {
					panic(err)
				}
			}()

			pathHashToOpLocs := make(map[string][]*pb.CommitOperationLocations_OperationLocation, 0)
			for op := range opsOut {
				offset, length, err := s.opdatastorecommit.Write(in.GetOwnerUsername(), in.GetProjectId(), []byte(changedPathHash), op)
				if err != nil {
					panic(err)
				}

				var chunkHash *pb.ChunkHash
				if op.GetType() == pb.Operation_OpData {
					chunkHash = &pb.ChunkHash{
						Offset: op.GetChunk().GetOffset(),
						Length: op.GetChunk().GetLength(),
						Hash:   op.GetChunk().GetHash(),
					}
				} else {
					chunkHash = &pb.ChunkHash{
						Offset: op.GetChunkHash().GetOffset(),
						Length: op.GetChunkHash().GetLength(),
						Hash:   op.GetChunkHash().GetHash(),
					}
				}

				if op.GetType() == pb.Operation_OpBlock {
					opLocs, err := s.oplocstorecommit.ListOperationLocations(in.GetOwnerUsername(), in.GetProjectId(), prevCommitId, []byte(changedPathHash))
					if err != nil {
						panic(err)
					}
					found := false
					var reusedOffset, reusedLength uint64
					for _, loc := range opLocs.GetOpLocs() {
						if loc.GetChunkHash().GetHash() == op.GetChunkHash().GetHash() {
							found = true
							reusedOffset = loc.GetOffset()
							reusedLength = loc.GetLength()
							break
						}
					}
					if !found {
						log.Fatal("Operation of type block but hash could not be found")
					}
					offset = reusedOffset
					length = reusedLength
				}

				operationLocation := &pb.CommitOperationLocations_OperationLocation{
					Offset:    offset,
					Length:    length,
					ChunkHash: chunkHash,
				}
				pathHashToOpLocs[string(changedPathHash)] = append(pathHashToOpLocs[string(changedPathHash)], operationLocation)
			}

			for pathHash, opLocs := range pathHashToOpLocs {
				err = s.oplocstorecommit.InsertOperationLocations(&pb.CommitOperationLocations{
					ProjectId:     in.GetProjectId(),
					OwnerUsername: in.GetOwnerUsername(),
					CommitId:      prevCommitId + 1,
					PathHash:      []byte(pathHash),
					OpLocs:        opLocs,
				})
				if err != nil {
					panic(err)
				}
			}

			results <- nil
		}
	}

	for i := 0; i < 64; i++ {
		go makeDiff()
	}

	go func() {
		for k := range changedPathHashes {
			pathHashes <- []byte(k)
		}
	}()

	completed := 0
	for e := range results {
		if e != nil {
			panic(e)
		}
		completed += 1

		if completed == len(changedPathHashes) {
			close(pathHashes)
			close(results)
		}
	}

	return &pb.MergeWorkspaceResponse{
		CommitId: prevCommitId + 1,
	}, nil
}
