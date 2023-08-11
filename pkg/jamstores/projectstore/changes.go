package projectstore

import "database/sql"

func (s *LocalStore) MaxWorkspaceChangeId(conn *sql.DB, ownerUsername string, projectId, workspaceId uint64) (uint64, error) {
	var maxId int64
	err := conn.QueryRow("SELECT MAX(change_id) FROM changes WHERE workspace_id = ?", workspaceId).Scan(&maxId)
	return uint64(maxId), err
}

func (s *LocalStore) AddChange(conn *sql.DB, ownerUsername string, projectId uint64, workspaceId uint64) (uint64, error) {
	currId, err := s.MaxWorkspaceChangeId(conn, ownerUsername, projectId, workspaceId)
	if err != nil {
		return 0, err
	}

	_, err = conn.Exec("INSERT INTO changes(change_id, workspace_id, timestamp) VALUES (?, ?, datetime('now'))", currId+1, workspaceId)
	if err != nil {
		return 0, err
	}

	return currId + 1, err
}
