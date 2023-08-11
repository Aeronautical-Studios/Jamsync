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

func (s *LocalStore) GetLocalDB(ownerUsername string, projectId uint64, pathHash []byte) (*sql.DB, error) {
	err := os.MkdirAll(s.fileDir(ownerUsername, projectId, pathHash), os.ModePerm)
	if err != nil {
		log.Panic(err)
	}

	var conn *sql.DB
	conn, err = sql.Open("sqlite3", s.filePath(ownerUsername, projectId, pathHash)+"?cache=shared&mode=rwc")
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

func (s *LocalStore) filePath(ownerUsername string, projectId uint64, pathHash []byte) string {
	return fmt.Sprintf("jamdata/%s/%d/commitdatastore/%02X/%02X.db", ownerUsername, projectId, pathHash[:1], pathHash)
}

func (s *LocalStore) fileDir(ownerUsername string, projectId uint64, pathHash []byte) string {
	return fmt.Sprintf("jamdata/%s/%d/commitdatastore/%02X", ownerUsername, projectId, pathHash[:1])
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

func (s *LocalStore) Read(conn *sql.DB, hash uint64) ([]byte, error) {
	hashString := strconv.FormatUint(hash, 10)
	row := conn.QueryRow("SELECT data FROM hashes WHERE hash = ?", hashString)
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

func (s *LocalStore) AddProject(ownerId string, projectId uint64) error {
	return os.MkdirAll(fmt.Sprintf("jamdata/%s/%d/opdatacommit", ownerId, projectId), os.ModePerm)
}
