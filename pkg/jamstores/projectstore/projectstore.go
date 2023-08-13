package projectstore

import (
	"database/sql"
	"fmt"
	"os"
	"sync"
)

type LocalStore struct {
	mutex *sync.Mutex
}

func NewLocalStore() *LocalStore {
	return &LocalStore{}
}

func (s *LocalStore) GetLocalProjectDB(ownerUsername string, projectId uint64) (*sql.DB, error) {
	dir := fmt.Sprintf("jamdata/%s/%d", ownerUsername, projectId)
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return nil, err
	}

	var conn *sql.DB
	conn, err = sql.Open("sqlite3", "file:"+dir+"/jamproject.db?cache=shared&mode=rwc")
	if err != nil {
		return nil, err
	}
	conn.SetMaxOpenConns(1)

	sqlStmt := `
	CREATE TABLE IF NOT EXISTS commits (timestamp DATETIME DEFAULT CURRENT_TIMESTAMP);
	CREATE TABLE IF NOT EXISTS commit_chunk_hashes (commit_id INTEGER, path_hash BLOB, hash TEXT, offset INTEGER, length INTEGER);
	CREATE TABLE IF NOT EXISTS workspaces (name TEXT, baseCommitId INTEGER, deleted INTEGER, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP);
	CREATE TABLE IF NOT EXISTS changes (change_id INTEGER, workspace_id INTEGER, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP);
	CREATE TABLE IF NOT EXISTS workspace_chunk_hashes (workspace_id INTEGER, change_id INTEGER, path_hash BLOB, hash TEXT, offset INTEGER, length INTEGER);

	CREATE INDEX IF NOT EXISTS workspace_chunk_hashes_desc ON workspace_chunk_hashes(change_id DESC, path_hash, workspace_id);
	CREATE INDEX IF NOT EXISTS commit_chunk_hashes_desc ON commit_chunk_hashes(commit_id DESC, path_hash);
	`
	_, err = conn.Exec(sqlStmt)
	if err != nil {
		return nil, err
	}

	return conn, nil
}
