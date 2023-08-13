package workspacedatastore

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
)

type LocalStore struct{}

func NewLocalStore() *LocalStore {
	return &LocalStore{}
}

func (s *LocalStore) filePath(ownerId string, projectId, workspaceId uint64, pathHash []byte) string {
	return fmt.Sprintf("jamdata/%s/%d/workspacedatastore/%d/%02X/%02X.db", ownerId, projectId, workspaceId, pathHash[:1], pathHash)
}

func (s *LocalStore) fileDir(ownerId string, projectId, workspaceId uint64, pathHash []byte) string {
	return fmt.Sprintf("jamdata/%s/%d/workspacedatastore/%d/%02X", ownerId, projectId, workspaceId, pathHash[:1])
}

func (s *LocalStore) LocalDBExists(ownerUsername string, projectId uint64, workspaceId uint64, pathHash []byte) bool {
	_, err := os.Stat(s.filePath(ownerUsername, projectId, workspaceId, pathHash))
	return err == nil
}

func (s *LocalStore) GetLocalDB(ownerUsername string, projectId uint64, workspaceId uint64, pathHash []byte) (*sql.DB, error) {
	err := os.MkdirAll(s.fileDir(ownerUsername, projectId, workspaceId, pathHash), os.ModePerm)
	if err != nil {
		log.Panic(err)
	}

	var conn *sql.DB
	conn, err = sql.Open("sqlite3", s.filePath(ownerUsername, projectId, workspaceId, pathHash)+"?cache=shared&mode=rwc")
	if err != nil {
		panic(err)
	}
	conn.SetMaxOpenConns(1)

	sqlStmt := `
		CREATE TABLE IF NOT EXISTS hashes (hash TEXT, data TEXT);
		`
	_, err = conn.Exec(sqlStmt)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func (s *LocalStore) Read(conn *sql.DB, hash uint64) ([]byte, error) {
	hashString := strconv.FormatUint(hash, 10)
	row := conn.QueryRow("SELECT data FROM hashes WHERE hash = ?", hashString)
	if row.Err() != nil {
		return nil, row.Err()
	}

	var data []byte
	err := row.Scan(&data)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	return data, nil
}

func (s *LocalStore) HashExists(conn *sql.DB, hash uint64) bool {
	hashString := strconv.FormatUint(hash, 10)
	row := conn.QueryRow("SELECT 1 FROM hashes WHERE hash = ?", hashString)
	if row.Err() != nil {
		return false
	}

	var data int
	err := row.Scan(&data)
	return err == nil
}

func (s *LocalStore) Write(conn *sql.DB, hash uint64, data []byte) error {
	hashString := strconv.FormatUint(hash, 10)
	_, err := conn.Exec("INSERT INTO hashes (hash, data) VALUES(?, ?)", hashString, data)
	return err
}

func (s *LocalStore) GetChunkHashes(conn *sql.DB, pathHash []byte) ([]uint64, error) {
	rows, err := conn.Query("SELECT hash FROM hashes")
	if err != nil {
		// Cant open (does not exist)
		return nil, nil
	}
	defer rows.Close()

	hashes := make([]uint64, 0)
	for rows.Next() {
		var hashString string
		err = rows.Scan(&hashString)
		if err != nil {
			return nil, err
		}
		hash, err := strconv.ParseUint(hashString, 10, 64)
		if err != nil {
			return nil, err
		}
		hashes = append(hashes, hash)
	}

	return hashes, err
}

func (s *LocalStore) DeleteWorkspace(ownerId string, projectId uint64, workspaceId uint64) error {
	dirs, err := os.ReadDir(fmt.Sprintf("jamdata/%s/%d/workspacedatastore/%d", ownerId, projectId, workspaceId))
	if err != nil {
		return err
	}

	for _, dir := range dirs {
		err := os.RemoveAll(fmt.Sprintf("jamdata/%s/%d/workspacedatastore/%d/%s", ownerId, projectId, workspaceId, dir.Name()))
		if err != nil {
			return err
		}
	}
	return nil
}
