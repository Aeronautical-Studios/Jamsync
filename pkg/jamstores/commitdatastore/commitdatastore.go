package commitdatastore

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

func (s *LocalStore) GetLocalDB(ownerUsername string, projectId uint64) (*sql.DB, error) {
	err := os.MkdirAll(s.fileDir(ownerUsername, projectId), os.ModePerm)
	if err != nil {
		log.Panic(err)
	}

	var conn *sql.DB
	conn, err = sql.Open("sqlite3", s.filePath(ownerUsername, projectId)+"?cache=shared&mode=rwc&_journal=WAL")
	if err != nil {
		panic(err)
	}

	sqlStmt := `
		CREATE TABLE IF NOT EXISTS hashes (path_hash BLOB, hash TEXT, data TEXT);
		CREATE INDEX IF NOT EXISTS path_hash_hash_idx ON hashes(path_hash, hash);
		CREATE INDEX IF NOT EXISTS path_hash_idx ON hashes(path_hash);
		`
	_, err = conn.Exec(sqlStmt)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func (s *LocalStore) filePath(ownerUsername string, projectId uint64) string {
	return fmt.Sprintf("jamdata/%s/%d/commitdatastore/commitdatastore.db", ownerUsername, projectId)
}

func (s *LocalStore) fileDir(ownerUsername string, projectId uint64) string {
	return fmt.Sprintf("jamdata/%s/%d/commitdatastore/", ownerUsername, projectId)
}

func (s *LocalStore) GetChunkHashes(conn *sql.DB, pathHash []byte) ([]uint64, error) {
	rows, err := conn.Query("SELECT hash FROM hashes WHERE path_hash = ?", pathHash)
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

func (s *LocalStore) Read(stmt *sql.Stmt, pathHash []byte, hash uint64) ([]byte, error) {
	hashString := strconv.FormatUint(hash, 10)
	row := stmt.QueryRow(pathHash, hashString)
	if row.Err() != nil {
		return nil, row.Err()
	}

	var data []byte
	err := row.Scan(&data)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (s *LocalStore) HashExists(conn *sql.DB, pathHash []byte, hash uint64) bool {
	hashString := strconv.FormatUint(hash, 10)
	row := conn.QueryRow("SELECT 1 FROM hashes WHERE path_hash = ? AND hash = ?", pathHash, hashString)
	if row.Err() != nil {
		return false
	}

	var data int
	err := row.Scan(&data)
	return err == nil
}

func (s *LocalStore) Write(stmt *sql.Stmt, pathHash []byte, hash uint64, data []byte) error {
	hashString := strconv.FormatUint(hash, 10)
	_, err := stmt.Exec(pathHash, hashString, data)
	return err
}
