package jamgrpc

import (
	"bytes"
	"context"
	"errors"
	"log"
	"os"

	"github.com/zdgeier/jam/gen/jampb"
	"github.com/zdgeier/jam/pkg/fastcdc"
	"github.com/zdgeier/jam/pkg/jamgrpc/serverauth"
)

func (s JamHub) GetProjectCurrentCommit(ctx context.Context, in *jampb.GetProjectCurrentCommitRequest) (*jampb.GetProjectCurrentCommitResponse, error) {
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
		return nil, errors.New("must be an owner or collaborator to get current commit")
	}

	commitId, err := s.oplocstorecommit.MaxCommitId(in.GetOwnerUsername(), in.ProjectId)
	if err != nil {
		return nil, err
	}

	return &jampb.GetProjectCurrentCommitResponse{
		CommitId: commitId,
	}, err
}

func (s JamHub) ReadCommitChunkHashes(ctx context.Context, in *jampb.ReadCommitChunkHashesRequest) (*jampb.ReadCommitChunkHashesResponse, error) {
	userId, err := serverauth.ParseIdFromCtx(ctx)
	if err != nil {
		if in.GetProjectId() != 1 {
			return nil, err
		}
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
		return nil, errors.New("must be an owner or collaborator to get current commit")
	}

	targetBuffer, err := s.regenCommittedFile(in.GetOwnerUsername(), in.GetProjectId(), in.GetCommitId(), in.GetPathHash())
	if err != nil {
		return nil, err
	}

	targetChunker, err := fastcdc.NewJamChunker(targetBuffer)
	if err != nil {
		return nil, err
	}
	sig := make([]*jampb.ChunkHash, 0)
	err = targetChunker.CreateSignature(func(ch *jampb.ChunkHash) error {
		sig = append(sig, ch)
		return nil
	})
	return &jampb.ReadCommitChunkHashesResponse{
		ChunkHashes: sig,
	}, err
}

func (s JamHub) regenCommittedFile(ownerUsername string, projectId uint64, commitId uint64, pathHash []byte) (*bytes.Reader, error) {
	var err error
	var operationLocations *jampb.CommitOperationLocations
	for i := int(commitId); i >= 0 && operationLocations == nil; i-- {
		operationLocations, err = s.oplocstorecommit.ListOperationLocations(ownerUsername, projectId, uint64(i), pathHash)
		if err != nil {
			return nil, err
		}
	}
	if operationLocations == nil {
		return bytes.NewReader([]byte{}), nil
	}

	ops := make(chan *jampb.Operation)
	go func() {
		for _, loc := range operationLocations.GetOpLocs() {
			op, err := s.opdatastorecommit.Read(ownerUsername, projectId, pathHash, loc.GetOffset(), loc.GetLength())
			if err != nil {
				log.Panic(err)
			}
			ops <- op
		}
		close(ops)
	}()

	result := new(bytes.Buffer)
	resultChunker, err := fastcdc.NewJamChunker(result)
	if err != nil {
		log.Panic(err)
	}
	targetBuffer := bytes.NewBuffer([]byte{})
	err = resultChunker.ApplyDelta(result, bytes.NewReader(targetBuffer.Bytes()), ops)
	if err != nil {
		log.Panic(err)
	}

	return bytes.NewReader(result.Bytes()), nil
}

func (s JamHub) ReadCommittedFile(in *jampb.ReadCommittedFileRequest, srv jampb.JamHub_ReadCommittedFileServer) error {
	userId, err := serverauth.ParseIdFromCtx(srv.Context())
	if err != nil {
		return err
	}

	if in.GetOwnerUsername() == "" {
		return errors.New("must provide owner id")
	}
	username, err := s.db.GetUsername(userId)
	if err != nil {
		return err
	}

	accessible, err := s.ProjectIdAccessible(in.GetOwnerUsername(), in.GetProjectId(), username)
	if err != nil {
		return err
	}

	if !accessible {
		return errors.New("not a collaborator or owner of this project")
	}

	commitId := in.GetCommitId()
	if commitId == 0 {
		maxCommitId, err := s.oplocstorecommit.MaxCommitId(in.GetOwnerUsername(), in.GetProjectId())
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		commitId = maxCommitId
	}

	sourceBuffer, err := s.regenCommittedFile(in.GetOwnerUsername(), in.GetProjectId(), commitId, in.GetPathHash())
	if err != nil {
		return err
	}

	sourceChunker, err := fastcdc.NewJamChunker(sourceBuffer)
	if err != nil {
		return err
	}

	opsOut := make(chan *jampb.Operation)
	tot := 0
	go func() {
		var blockCt, dataCt, bytes int
		defer close(opsOut)
		err := sourceChunker.CreateDelta(in.GetChunkHashes(), func(op *jampb.Operation) error {
			tot += int(op.Chunk.GetLength()) + int(op.ChunkHash.GetLength())
			switch op.Type {
			case jampb.Operation_OpBlock:
				blockCt++
			case jampb.Operation_OpData:
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
		err = srv.Send(&jampb.CommittedFileOperation{
			ProjectId:     in.GetProjectId(),
			OwnerUsername: in.GetOwnerUsername(),
			PathHash:      in.GetPathHash(),
			Op:            op,
		})
		if err != nil {
			return err
		}
	}
	return nil
}
