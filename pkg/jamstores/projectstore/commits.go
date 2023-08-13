package projectstore

import "database/sql"

func (s *LocalStore) AddCommit(db *sql.DB) (uint64, error) {
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

func (s *LocalStore) MaxCommitId(db *sql.DB) (uint64, error) {
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
