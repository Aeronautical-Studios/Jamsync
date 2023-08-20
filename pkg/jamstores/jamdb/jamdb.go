package jamdb

import (
	"database/sql"
	"errors"
	"log"
	"os"
)

type LocalStore struct {
	db *sql.DB
}

type FileLock struct {
	ProjectId      uint64
	Username       string
	B64EncodedPath string
	IsDir          bool
}

func NewLocalStore() (db LocalStore) {
	err := os.MkdirAll("./jamdata", os.ModePerm)
	if err != nil {
		panic(err)
	}
	conn, err := sql.Open("sqlite3", "./jamdata/jam.db")
	if err != nil {
		panic(err)
	}

	sqlStmt := `
	CREATE TABLE IF NOT EXISTS users (username TEXT, user_id TEXT, UNIQUE(username, user_id));
	CREATE TABLE IF NOT EXISTS projects (name TEXT, owner_username TEXT, UNIQUE(name, owner_username));
	CREATE TABLE IF NOT EXISTS collaborators (project_id INTEGER, username TEXT, UNIQUE(project_id, username));
	CREATE TABLE IF NOT EXISTS filelocks (project_id INTEGER, username TEXT, file_hash TEXT, is_dir BOOL, UNIQUE(project_id, username, file_hash));
	CREATE TABLE IF NOT EXISTS operation_stream_tokens (token TEXT, owner_username TEXT, project_id INTEGER, workspace_id INTEGER, change_id INTEGER, expires TIMESTAMP DEFAULT (DATETIME(CURRENT_TIMESTAMP, '+5 hours')), UNIQUE(token));
	`
	_, err = conn.Exec(sqlStmt)
	if err != nil {
		panic(err)
	}

	return LocalStore{db: conn}
}

type Project struct {
	OwnerUsername string
	Name          string
	Id            uint64
}

func (j *LocalStore) AddOperationStreamToken(ownerUsername string, projectId, workspaceId, changeId uint64, token []byte) error {
	_, err := j.db.Exec("DELETE FROM operation_stream_tokens WHERE expires < CURRENT_TIMESTAMP")
	if err != nil {
		return err
	}

	_, err = j.db.Exec("INSERT INTO operation_stream_tokens (token, owner_username, project_id, workspace_id, change_id) VALUES (?, ?, ?, ?, ?)", token, ownerUsername, projectId, workspaceId, changeId)
	return err
}

func (j *LocalStore) GetOperationStreamTokenInfo(token []byte) (ownerUsername string, projectId, workspaceId, changeId uint64, err error) {
	err = j.db.QueryRow("SELECT owner_username, project_id, workspace_id, change_id FROM operation_stream_tokens WHERE token = ?", token).Scan(&ownerUsername, &projectId, &workspaceId, &changeId)
	if err != nil {
		return "", 0, 0, 0, err
	}
	return ownerUsername, projectId, workspaceId, changeId, nil
}

func (j LocalStore) AddProject(projectName string, ownerUsername string) (uint64, error) {
	_, err := j.GetProjectId(projectName, ownerUsername)
	if !errors.Is(sql.ErrNoRows, err) {
		return 0, err
	}

	res, err := j.db.Exec("INSERT INTO projects(name, owner_username) VALUES(?, ?)", projectName, ownerUsername)
	if err != nil {
		return 0, err
	}

	var id int64
	if id, err = res.LastInsertId(); err != nil {
		return 0, err
	}

	return uint64(id), nil
}

func (j LocalStore) AddCollaborator(projectId uint64, collabUsername string) error {
	_, err := j.db.Exec("INSERT OR IGNORE INTO collaborators(project_id, username) VALUES(?, ?)", projectId, collabUsername)
	return err
}

func (j LocalStore) ListCollaborators(projectId uint64) ([]string, error) {
	rows, err := j.db.Query("SELECT username FROM collaborators WHERE project_id = ?", projectId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	data := make([]string, 0)
	for rows.Next() {
		var u string
		err = rows.Scan(&u)
		if err != nil {
			return nil, err
		}
		data = append(data, u)
	}
	return data, err
}

func (j LocalStore) HasCollaborator(projectId uint64, collaborator string) bool {
	var username string
	err := j.db.QueryRow(`SELECT username FROM collaborators WHERE project_id = ? AND username = ?;`, projectId, collaborator).Scan(&username)
	if err != nil {
		if err != sql.ErrNoRows {
			log.Print(err)
		}

		return false
	}

	return true
}

func (j LocalStore) GetUserId(username string) (string, error) {
	row := j.db.QueryRow("SELECT user_id FROM users WHERE username = ?", username)
	if row.Err() != nil {
		return "", row.Err()
	}

	var userId string
	err := row.Scan(&userId)
	return userId, err
}

func (j LocalStore) GetUsername(user_id string) (string, error) {
	row := j.db.QueryRow("SELECT username FROM users WHERE user_id = ?", user_id)
	if row.Err() != nil {
		return "", row.Err()
	}

	var userId string
	err := row.Scan(&userId)
	return userId, err
}

func (j LocalStore) GetProjectOwnerUsername(projectId uint64) (string, error) {
	row := j.db.QueryRow("SELECT owner_username FROM projects WHERE rowid = ?", projectId)
	if row.Err() != nil {
		return "", row.Err()
	}

	var ownerUsername string
	err := row.Scan(&ownerUsername)
	return ownerUsername, err
}

func (j LocalStore) GetProjectId(projectName string, ownerUsername string) (uint64, error) {
	row := j.db.QueryRow("SELECT rowid FROM projects WHERE name = ? AND owner_username = ?", projectName, ownerUsername)
	if row.Err() != nil {
		return 0, row.Err()
	}

	var id uint64
	err := row.Scan(&id)
	return id, err
}

func (j LocalStore) GetProjectName(id uint64, ownerUsername string) (string, error) {
	row := j.db.QueryRow("SELECT name FROM projects WHERE rowid = ? AND owner_username = ?", id, ownerUsername)
	if row.Err() != nil {
		return "", row.Err()
	}

	var name string
	err := row.Scan(&name)
	return name, err
}

func (j LocalStore) ListProjectsOwned(ownerUsername string) ([]Project, error) {
	rows, err := j.db.Query("SELECT rowid, name, owner_username FROM projects WHERE owner_username = ?", ownerUsername)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	data := make([]Project, 0)
	for rows.Next() {
		u := Project{}
		err = rows.Scan(&u.Id, &u.Name, &u.OwnerUsername)
		if err != nil {
			return nil, err
		}
		data = append(data, u)
	}
	return data, err
}

func (j LocalStore) ListProjectsAsCollaborator(username string) ([]Project, error) {
	rows, err := j.db.Query("SELECT p.rowid, p.name, p.owner_username from projects AS p INNER JOIN collaborators AS c WHERE p.rowid = c.project_id AND c.username = ?", username)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	data := make([]Project, 0)
	for rows.Next() {
		u := Project{}
		err = rows.Scan(&u.Id, &u.Name, &u.OwnerUsername)
		if err != nil {
			return nil, err
		}
		data = append(data, u)
	}
	return data, err
}

func (j LocalStore) CreateUser(username, userId string) error {
	_, err := j.db.Exec("INSERT OR IGNORE INTO users(username, user_id) VALUES (?, ?)", username, userId)
	return err
}

func (j LocalStore) Username(userId string) (string, error) {
	row := j.db.QueryRow("SELECT username FROM users WHERE user_id = ?", userId)
	if row.Err() != nil {
		return "", row.Err()
	}

	var username string
	err := row.Scan(&username)
	return username, err
}

func (j LocalStore) UserId(username string) (string, error) {
	row := j.db.QueryRow("SELECT user_id FROM users WHERE username = ?", username)
	if row.Err() != nil {
		return "", row.Err()
	}

	var userId string
	err := row.Scan(&username)
	return userId, err
}

func (j LocalStore) CreateFileLock(projectId uint64, username string, b64EncodedPath string, isDir bool) error {
	rows, err := j.db.Query("SELECT username FROM filelocks WHERE project_id = ? AND username != ? AND file_hash = ?", projectId, username, b64EncodedPath)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var username string
		err = rows.Scan(&username)
		if err != nil {
			return err
		}

		if username != "" {
			return errors.New("File lock is already owned by another user")
		}
	}

	_, err = j.db.Exec("INSERT OR REPLACE INTO filelocks(project_id, username, file_hash, is_dir) VALUES(?, ?, ?, ?)", projectId, username, b64EncodedPath, isDir)
	if err != nil {
		return err
	}

	return nil
}

func (j LocalStore) GetFileLock(projectId uint64, username string, b64EncodedPath string) (bool, error) {
	row := j.db.QueryRow("SELECT EXISTS(SELECT 1 FROM filelocks WHERE project_id = ? AND username = ? AND file_hash = ?)", projectId, username, b64EncodedPath)
	if row.Err() != nil {
		return false, row.Err()
	}

	var exists bool
	err := row.Scan(&exists)
	return exists, err
}

func (j LocalStore) ListFileLocks(projectId uint64) ([]FileLock, error) {
	rows, err := j.db.Query("SELECT project_id, username, file_hash, is_dir FROM filelocks WHERE project_id = ?", projectId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	data := make([]FileLock, 0)
	for rows.Next() {
		u := FileLock{}
		err = rows.Scan(&u.ProjectId, &u.Username, &u.B64EncodedPath, &u.IsDir)
		if err != nil {
			return nil, err
		}
		data = append(data, u)
	}
	return data, err
}

func (j LocalStore) DeleteFileLock(projectId uint64, username string, b64EncodedPath string) error {
	rows, err := j.db.Query("SELECT username FROM filelocks WHERE project_id = ? AND username != ? AND file_hash = ?", projectId, username, b64EncodedPath)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var username string
		err = rows.Scan(&username)
		if err != nil {
			return err
		}

		if username != "" {
			return errors.New("File lock is already owned by another user")
		}
	}

	_, err = j.db.Exec("DELETE FROM filelocks WHERE project_id = ? AND username = ? AND file_hash = ?", projectId, username, b64EncodedPath)
	if err != nil {
		return err
	}

	return nil
}
