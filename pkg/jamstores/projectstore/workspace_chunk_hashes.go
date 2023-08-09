package projectstore

import (
	"database/sql"
	"strconv"

	"github.com/zdgeier/jam/gen/jampb"
)

func (s *LocalStore) InsertWorkspaceChunkHashes(db *sql.DB, ownerUsername string, projectId, workspaceId, changeId uint64, pathHash []byte, chunkHashes []*jampb.ChunkHash) error {
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

func (s *LocalStore) ListWorkspaceChangedPathHashes(db *sql.DB, ownerUsername string, projectId, workspaceId uint64) (map[string]interface{}, error) {
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

func (s *LocalStore) ListWorkspaceChunkHashes(db *sql.DB, ownerUsername string, projectId, workspaceId, changeId uint64, pathHash []byte) ([]*jampb.ChunkHash, error) {
	rows, err := db.Query(`
		SELECT hash, offset, length FROM workspace_chunk_hashes
		WHERE change_id = (
			SELECT MAX(change_id) FROM workspace_chunk_hashes
			WHERE path_hash = ? AND workspace_id = ? AND change_id <= ?
		) AND path_hash = ? AND workspace_id = ? AND change_id <= ?;
	`, pathHash, workspaceId, changeId, pathHash, workspaceId, changeId)
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
		baseCommitId, err := s.GetWorkspaceBaseCommitId(db, ownerUsername, projectId, workspaceId)
		if err != nil {
			return nil, err
		}
		return s.ListCommitChunkHashes(db, ownerUsername, projectId, baseCommitId, pathHash)
	}

	return chunkHashes, nil
}
