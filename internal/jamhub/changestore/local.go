package changestore

import (
	"database/sql"
	"fmt"
	"os"
)

type LocalChangeStore struct {
	dbs map[uint64]*sql.DB
}

func NewLocalChangeStore() LocalChangeStore {
	return LocalChangeStore{
		dbs: make(map[uint64]*sql.DB, 0),
	}
}

func (s LocalChangeStore) getLocalProjectDB(ownerUsername string, projectId uint64) (*sql.DB, error) {
	if db, ok := s.dbs[projectId]; ok {
		return db, nil
	}

	dir := fmt.Sprintf("jamhubdata/%s/%d", ownerUsername, projectId)
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return nil, err
	}

	localDB, err := sql.Open("sqlite3", dir+"/jamhubproject.db")
	if err != nil {
		return nil, err
	}
	err = setup(localDB)
	if err != nil {
		return nil, err
	}

	s.dbs[projectId] = localDB
	return localDB, nil
}

func (s LocalChangeStore) GetWorkspaceNameById(ownerUsername string, projectId uint64, workspaceId uint64) (string, error) {
	db, err := s.getLocalProjectDB(ownerUsername, projectId)
	if err != nil {
		return "", err
	}
	return getWorkspaceNameById(db, workspaceId)
}

func (s LocalChangeStore) GetWorkspaceIdByName(ownerUsername string, projectId uint64, workspaceName string) (uint64, error) {
	db, err := s.getLocalProjectDB(ownerUsername, projectId)
	if err != nil {
		return 0, err
	}
	return getWorkspaceIdByName(db, workspaceName)
}

func (s LocalChangeStore) GetWorkspaceBaseCommitId(ownerUsername string, projectId uint64, workspaceId uint64) (uint64, error) {
	db, err := s.getLocalProjectDB(ownerUsername, projectId)
	if err != nil {
		return 0, err
	}
	return getWorkspaceBaseCommitId(db, workspaceId)
}

func (s LocalChangeStore) DeleteWorkspace(ownerUsername string, projectId uint64, workspaceId uint64) error {
	db, err := s.getLocalProjectDB(ownerUsername, projectId)
	if err != nil {
		return err
	}
	return deleteWorkspace(db, workspaceId)
}

func (s LocalChangeStore) UpdateWorkspaceBaseCommit(ownerUsername string, projectId uint64, workspaceId uint64, baseCommitId uint64) error {
	db, err := s.getLocalProjectDB(ownerUsername, projectId)
	if err != nil {
		return err
	}
	return updateWorkspaceBaseCommit(db, workspaceId, baseCommitId)
}

func (s LocalChangeStore) AddWorkspace(ownerUsername string, projectId uint64, workspaceName string, commitId uint64) (uint64, error) {
	db, err := s.getLocalProjectDB(ownerUsername, projectId)
	if err != nil {
		return 0, err
	}
	return addWorkspace(db, workspaceName, commitId)
}

func (s LocalChangeStore) ListWorkspaces(ownerUsername string, projectId uint64) (map[string]uint64, error) {
	db, err := s.getLocalProjectDB(ownerUsername, projectId)
	if err != nil {
		return nil, err
	}

	return listWorkspaces(db)
}

func (s LocalChangeStore) DeleteProject(projectId uint64, ownerUsername string) error {
	return os.RemoveAll(fmt.Sprintf("jamhubdata/%s/%d", ownerUsername, projectId))
}
