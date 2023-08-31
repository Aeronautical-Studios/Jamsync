package stores

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/zdgeier/jam/gen/jampb"
)

func GetProjectDB(ownerUsername string, projectId uint64) (*sql.DB, error) {
	dir := fmt.Sprintf("jamdata/%s/%d", ownerUsername, projectId)
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return nil, err
	}

	var conn *sql.DB
	conn, err = sql.Open("sqlite3", "file:"+dir+"/jamproject.db?cache=shared&mode=rwc&_journal=WAL")
	if err != nil {
		return nil, err
	}

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

	_, err = conn.Exec(fmt.Sprintf("ATTACH 'jamdata/%s/%d/commitdatastore.db' AS commitdatastore", ownerUsername, projectId))
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func AddCommit(db *sql.DB) (uint64, error) {
	res, err := db.Exec("INSERT INTO commits (timestamp) VALUES (datetime('now'))")
	if err != nil {
		return 0, err
	}

	r, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}

	return uint64(r), err
}

func MaxCommitId(db *sql.DB) (uint64, error) {
	row := db.QueryRow("SELECT MAX(rowid) FROM commits")
	if row.Err() != nil {
		return 0, row.Err()
	}

	var maxId int64
	err := row.Scan(&maxId)
	if err != nil {
		return 0, nil // could be null when project is new
	}
	return uint64(maxId), err
}

func ListChangedPathHashesFromCommit(db *sql.DB, commitId uint64) (map[string]interface{}, error) {
	rows, err := db.Query("SELECT DISTINCT path_hash FROM commit_chunk_hashes WHERE rowid > ?", commitId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	pathHashes := make(map[string]interface{})
	for rows.Next() {
		var pathHash []byte
		err = rows.Scan(&pathHash)
		if err != nil {
			return nil, err
		}
		pathHashes[string(pathHash)] = nil
	}

	return pathHashes, nil
}

func InsertCommitChunkHashes(db *sql.DB, commitId uint64, pathHash []byte, chunkHashes []*jampb.ChunkHash) error {
	insertStmt, err := db.Prepare("INSERT INTO commit_chunk_hashes (commit_id, path_hash, hash, offset, length) VALUES (?, ?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer insertStmt.Close()

	for _, row := range chunkHashes {
		hashString := strconv.FormatUint(row.Hash, 10)
		_, err = insertStmt.Exec(int64(commitId), pathHash, hashString, int64(row.Offset), int64(row.Length))
		if err != nil {
			return err
		}
	}
	return nil
}

func ListCommitChunkHashes(db *sql.DB, commitId uint64, pathHash []byte) ([]*jampb.ChunkHash, error) {
	rows, err := db.Query(`
		SELECT hash, offset, length FROM commit_chunk_hashes
		WHERE commit_id = (
			SELECT MAX(commit_id) FROM commit_chunk_hashes WHERE commit_id <= ? AND path_hash = ?
		) AND commit_id <= ? AND path_hash = ?;
	`, commitId, pathHash, commitId, pathHash)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	chunkHashes := make([]*jampb.ChunkHash, 0)
	for rows.Next() {
		var hashString string
		var offset, length int64
		err = rows.Scan(&hashString, &offset, &length)
		if err != nil {
			return nil, err
		}
		hash, err := strconv.ParseUint(hashString, 10, 64)
		if err != nil {
			return nil, err
		}
		chunkHashes = append(chunkHashes, &jampb.ChunkHash{
			Hash:   uint64(hash),
			Offset: uint64(offset),
			Length: uint64(length),
		})
	}
	return chunkHashes, nil
}

func MaxWorkspaceChangeId(conn *sql.DB, ownerUsername string, projectId, workspaceId uint64) (uint64, error) {
	var maxId int64
	err := conn.QueryRow("SELECT MAX(change_id) FROM changes WHERE workspace_id = ?", workspaceId).Scan(&maxId)
	return uint64(maxId), err
}

func AddChange(conn *sql.DB, ownerUsername string, projectId uint64, workspaceId uint64) (uint64, error) {
	currId, err := MaxWorkspaceChangeId(conn, ownerUsername, projectId, workspaceId)
	if err != nil {
		return 0, err
	}

	_, err = conn.Exec("INSERT INTO changes(change_id, workspace_id, timestamp) VALUES (?, ?, datetime('now'))", currId+1, workspaceId)
	if err != nil {
		return 0, err
	}

	return currId + 1, err
}

func InsertWorkspaceChunkHash(db *sql.DB, workspaceId, changeId uint64, pathHash []byte, chunkHash *jampb.ChunkHash) error {
	insertStmt, err := db.Prepare("INSERT INTO workspace_chunk_hashes (workspace_id, change_id, path_hash, hash, offset, length) VALUES (?, ?, ?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer insertStmt.Close()

	hashString := strconv.FormatUint(chunkHash.Hash, 10)
	_, err = insertStmt.Exec(int64(workspaceId), int64(changeId), pathHash, hashString, int64(chunkHash.Offset), int64(chunkHash.Length))
	if err != nil {
		return err
	}
	return err
}

func InsertWorkspaceChunkHashes(db *sql.DB, workspaceId, changeId uint64, pathHash []byte, chunkHashes []*jampb.ChunkHash) error {
	insertStmt, err := db.Prepare("INSERT INTO workspace_chunk_hashes (workspace_id, change_id, path_hash, hash, offset, length) VALUES (?, ?, ?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer insertStmt.Close()

	for _, row := range chunkHashes {
		hashString := strconv.FormatUint(row.Hash, 10)
		_, err = insertStmt.Exec(int64(workspaceId), int64(changeId), pathHash, hashString, int64(row.Offset), int64(row.Length))
		if err != nil {
			return err
		}
	}

	return err
}

func ListWorkspaceChangedPathHashes(db *sql.DB, workspaceId uint64) (map[string]interface{}, error) {
	rows, err := db.Query("SELECT DISTINCT path_hash FROM workspace_chunk_hashes WHERE workspace_id = ?", workspaceId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	pathHashes := make(map[string]interface{})
	for rows.Next() {
		var pathHash []byte
		err = rows.Scan(&pathHash)
		if err != nil {
			return nil, err
		}
		pathHashes[string(pathHash)] = nil
	}

	return pathHashes, nil
}

func ListWorkspaceChunkHashes(db *sql.DB, stmt *sql.Stmt, workspaceId, changeId uint64, pathHash []byte) ([]*jampb.ChunkHash, error) {
	rows, err := stmt.Query(pathHash, workspaceId, changeId, pathHash, workspaceId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	chunkHashes := make([]*jampb.ChunkHash, 0)
	for rows.Next() {
		var hashString string
		var offset, length int64
		err = rows.Scan(&hashString, &offset, &length)
		if err != nil {
			return nil, err
		}
		hash, err := strconv.ParseUint(hashString, 10, 64)
		if err != nil {
			return nil, err
		}
		chunkHashes = append(chunkHashes, &jampb.ChunkHash{
			Hash:   uint64(hash),
			Offset: uint64(offset),
			Length: uint64(length),
		})
	}

	if len(chunkHashes) == 0 {
		baseCommitId, err := GetWorkspaceBaseCommitId(db, workspaceId)
		if err != nil {
			return nil, err
		}
		return ListCommitChunkHashes(db, baseCommitId, pathHash)
	}

	return chunkHashes, nil
}

func GetWorkspaceNameById(db *sql.DB, workspaceId uint64) (string, error) {
	row := db.QueryRow("SELECT name FROM workspaces WHERE rowid = ?", workspaceId)
	if row.Err() != nil {
		return "", row.Err()
	}

	var name string
	err := row.Scan(&name)
	if errors.Is(sql.ErrNoRows, err) {
		return "", nil
	}
	return name, err
}

func GetWorkspaceIdByName(db *sql.DB, workspaceName string) (uint64, error) {
	row := db.QueryRow("SELECT rowid FROM workspaces WHERE name = ?", workspaceName)
	if row.Err() != nil {
		return 0, row.Err()
	}

	var workspaceId uint64
	err := row.Scan(&workspaceId)
	if errors.Is(sql.ErrNoRows, err) {
		return 0, nil
	}
	return workspaceId, err
}

func GetWorkspaceBaseCommitId(db *sql.DB, workspaceId uint64) (uint64, error) {
	row := db.QueryRow("SELECT baseCommitId FROM workspaces WHERE rowid = ?", workspaceId)
	if row.Err() != nil {
		return 0, row.Err()
	}

	var commitId uint64
	err := row.Scan(&commitId)
	if errors.Is(sql.ErrNoRows, err) {
		return 0, nil
	}
	return commitId, err
}

func DeleteWorkspace(db *sql.DB, workspaceId uint64) error {
	_, err := db.Exec("UPDATE workspaces SET deleted = 1 WHERE rowid = ?", workspaceId)
	if err != nil {
		return err
	}

	return err
}

func UpdateWorkspaceBaseCommit(db *sql.DB, workspaceId uint64, baseCommitId uint64) error {
	_, err := db.Exec("UPDATE workspaces SET baseCommitId = ? WHERE rowid = ?", baseCommitId, workspaceId)
	if err != nil {
		return err
	}

	return err
}

func AddWorkspace(db *sql.DB, workspaceName string, baseCommitId uint64) (uint64, error) {
	tx, err := db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	res, err := tx.Exec("INSERT INTO workspaces(name, baseCommitId, deleted) VALUES(?, ?, 0)", workspaceName, baseCommitId)
	if err != nil {
		return 0, err
	}

	rowId, err := res.LastInsertId()
	if err != nil {
		return uint64(rowId), err
	}

	_, err = tx.Exec("INSERT INTO changes(workspace_id, change_id) VALUES(?, ?)", rowId, 0)
	if err != nil {
		return 0, err
	}

	err = tx.Commit()
	if err != nil {
		return 0, err
	}

	return uint64(rowId), err
}

func ListWorkspaces(db *sql.DB) (map[string]uint64, error) {
	rows, err := db.Query("SELECT rowid, name FROM workspaces WHERE deleted = 0")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	data := make(map[string]uint64, 0)
	for rows.Next() {
		var name string
		var id uint64
		err = rows.Scan(&id, &name)
		if err != nil {
			return nil, err
		}
		data[name] = id
	}

	return data, err
}
