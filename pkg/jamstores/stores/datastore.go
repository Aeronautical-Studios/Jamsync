package stores

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
)

func GetCommitDataDB(ownerUsername string, projectId uint64) (*sql.DB, error) {
	var conn *sql.DB
	conn, err := sql.Open("sqlite3", fmt.Sprintf("jamdata/%s/%d/commitdatastore.db", ownerUsername, projectId)+"?cache=shared&mode=rwc&_journal=WAL")
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

func GetWorkspaceDataDB(ownerUsername string, projectId uint64, workspaceId uint64) (*sql.DB, error) {
	err := os.MkdirAll(fmt.Sprintf("jamdata/%s/%d/workspacedatastore/%d", ownerUsername, projectId, workspaceId), os.ModePerm)
	if err != nil {
		log.Panic(err)
	}

	var conn *sql.DB
	conn, err = sql.Open("sqlite3", fmt.Sprintf("jamdata/%s/%d/workspacedatastore/%d/workspacedatastore.db", ownerUsername, projectId, workspaceId)+"?cache=shared&mode=rwc&_journal=WAL")
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

func DeleteWorkspaceDataDB(ownerUsername string, projectId uint64, workspaceId uint64) error {
	return os.Remove(fmt.Sprintf("jamdata/%s/%d/workspacedatastore/%d/workspacedatastore.db", ownerUsername, projectId, workspaceId))
}

func GetChunkHashes(db *sql.DB, pathHash []byte) ([]uint64, error) {
	rows, err := db.Query("SELECT hash FROM hashes WHERE path_hash = ?", pathHash)
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

func Read(stmt *sql.Stmt, pathHash []byte, hash uint64) ([]byte, error) {
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

func Write(stmt *sql.Stmt, pathHash []byte, hash uint64, data []byte) error {
	hashString := strconv.FormatUint(hash, 10)
	_, err := stmt.Exec(pathHash, hashString, data)
	return err
}
