package projectstore

import (
	"database/sql"
	"strconv"

	"github.com/zdgeier/jam/gen/jampb"
)

func (s *LocalStore) ListChangedPathHashesFromCommit(db *sql.DB, commitId uint64) (map[string]interface{}, error) {
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

func (s *LocalStore) InsertCommitChunkHashes(db *sql.DB, commitId uint64, pathHash []byte, chunkHashes []*jampb.ChunkHash) error {
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

func (s *LocalStore) ListCommitChunkHashes(db *sql.DB, commitId uint64, pathHash []byte) ([]*jampb.ChunkHash, error) {
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
