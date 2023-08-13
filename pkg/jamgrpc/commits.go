package jamgrpc

import (
	"context"
	"errors"

	"github.com/zdgeier/jam/gen/jampb"
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

	db, err := s.projectstore.GetLocalProjectDB(in.GetOwnerUsername(), in.GetProjectId())
	if err != nil {
		return nil, err
	}
	defer db.Close()

	commitId, err := s.projectstore.MaxCommitId(db)
	if err != nil {
		return nil, err
	}

	return &jampb.GetProjectCurrentCommitResponse{
		CommitId: commitId,
	}, err
}

func (s JamHub) ReadCommitFileHashes(ctx context.Context, in *jampb.ReadCommitFileHashesRequest) (*jampb.ReadCommitFileHashesResponse, error) {
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

	db, err := s.projectstore.GetLocalProjectDB(in.GetOwnerUsername(), in.GetProjectId())
	if err != nil {
		return nil, err
	}
	defer db.Close()

	hashList, err := s.projectstore.ListCommitChunkHashes(db, in.GetCommitId(), in.GetPathHash())
	if err != nil {
		return nil, err
	}

	hashes := make(map[uint64][]byte, 0)
	for _, hash := range hashList {
		hashes[hash.Hash] = nil
	}

	return &jampb.ReadCommitFileHashesResponse{
		Hashes: hashes,
	}, err
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

	accessible, err := s.ProjectIdAccessible(in.OwnerUsername, in.ProjectId, username)
	if err != nil {
		return err
	}

	if !accessible {
		return errors.New("not a collaborator or owner of this project")
	}

	db, err := s.projectstore.GetLocalProjectDB(in.GetOwnerUsername(), in.GetProjectId())
	if err != nil {
		return err
	}
	defer db.Close()

	actualChunkHashes, err := s.projectstore.ListCommitChunkHashes(db, in.CommitId, in.PathHash)
	if err != nil {
		return err
	}

	conn, err := s.commitdatastore.GetLocalDB(in.OwnerUsername, in.ProjectId, in.PathHash)
	if err != nil {
		return err
	}
	defer conn.Close()

	for _, actualChunk := range actualChunkHashes {
		if _, ok := in.LocalChunkHashes[actualChunk.Hash]; ok {
			err = srv.Send(&jampb.FileReadOperation{
				PathHash: in.PathHash,
				Chunk: &jampb.Chunk{
					Hash:   actualChunk.Hash,
					Offset: actualChunk.Offset,
					Length: actualChunk.Length,
				},
			})
			if err != nil {
				return err
			}
		} else {
			data, err := s.commitdatastore.Read(conn, actualChunk.Hash)
			if err != nil {
				return err
			}
			err = srv.Send(&jampb.FileReadOperation{
				PathHash: in.PathHash,
				Chunk: &jampb.Chunk{
					Hash:   actualChunk.Hash,
					Offset: actualChunk.Offset,
					Length: actualChunk.Length,
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
