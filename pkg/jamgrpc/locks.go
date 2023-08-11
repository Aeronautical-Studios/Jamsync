package jamgrpc

import (
	"context"
	"errors"

	"github.com/zdgeier/jam/gen/jampb"
	"github.com/zdgeier/jam/pkg/jamgrpc/serverauth"
)

func (s JamHub) UpdateFileLock(ctx context.Context, in *jampb.UpdateFileLockRequest) (*jampb.UpdateFileLockResponse, error) {
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
		return nil, errors.New("must be an owner or collaborator to update file lock")
	}

	if in.GetLockUnlockFlag() {
		err = s.db.CreateFileLock(in.GetProjectId(), username, in.GetB64EncodedPath(), in.GetIsDir())
	} else {
		err = s.db.DeleteFileLock(in.GetProjectId(), username, in.GetB64EncodedPath())
	}

	if err != nil {
		return nil, err
	}
	
	return &jampb.UpdateFileLockResponse{
		IsLocked: in.GetLockUnlockFlag(),
	}, err
}

func (s JamHub) GetFileLock(ctx context.Context, in *jampb.GetFileLockRequest) (*jampb.GetFileLockResponse, error) {
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
		return nil, errors.New("must be an owner or collaborator to get file lock")
	}

	isLocked, err := s.db.GetFileLock(in.GetProjectId(), username, in.GetB64EncodedPath())
	if err != nil {
		return nil, err
	}

	return &jampb.GetFileLockResponse{
		IsLocked: isLocked,
	}, nil
}

func (s JamHub) ListFileLocks(ctx context.Context, in *jampb.ListFileLocksRequest) (*jampb.ListFileLocksResponse, error) {
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
		return nil, errors.New("must be an owner or collaborator to get file locks")
	}

	lockedFiles, err := s.db.ListFileLocks(in.GetProjectId())
	if err != nil {
		return nil, err
	}

	// data from lockedFiles array into []*jampb.FileLock
	lockedFilesProto := make([]*jampb.FileLock, len(lockedFiles))
	for i, lock := range lockedFiles {
		lockedFilesProto[i] = &jampb.FileLock{
			ProjectId: lock.ProjectId,
			OwnerUsername: lock.Username,
			B64EncodedPath: lock.B64EncodedPath,
			IsDir: lock.IsDir,
		}
	}

	return &jampb.ListFileLocksResponse{
		LockedFiles: lockedFilesProto,
	}, nil
}

// deleteFileLock deletes a file lock for a project
func (s JamHub) deleteFileLock(projectId uint64, username string, b64EncodedPath string) error {
	return s.db.DeleteFileLock(projectId, username, b64EncodedPath)
}

// deleteFileLocks deletes all file locks for a project
func (s JamHub) deleteFileLocks(projectId uint64) error {
	locks, err := s.db.ListFileLocks(projectId)
	if err != nil {
		return err
	}

	for _, lock := range locks {
		s.db.DeleteFileLock(projectId, lock.Username, lock.B64EncodedPath)
	}

	return nil
}
