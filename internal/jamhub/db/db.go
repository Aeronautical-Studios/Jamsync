package db

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
)

type JamHubDb struct {
	db *sql.DB
}

func New() (jamhubDB JamHubDb) {
	err := os.MkdirAll("./jamhubdata", os.ModePerm)
	if err != nil {
		panic(err)
	}
	conn, err := sql.Open("sqlite3", "./jamhubdata/jamhub.db")
	if err != nil {
		panic(err)
	}

	sqlStmt := `
	CREATE TABLE IF NOT EXISTS users (username TEXT, user_id TEXT, UNIQUE(username, user_id));
	CREATE TABLE IF NOT EXISTS projects (name TEXT, owner_username TEXT, UNIQUE(name, owner_username));
	CREATE TABLE IF NOT EXISTS collaborators (project_id INTEGER, username TEXT, UNIQUE(project_id, username));
	`
	_, err = conn.Exec(sqlStmt)
	if err != nil {
		panic(err)
	}

	return JamHubDb{conn}
}

type Project struct {
	OwnerUsername string
	Name          string
	Id            uint64
}

func (j JamHubDb) AddProject(projectName string, ownerUsername string) (uint64, error) {
	_, err := j.GetProjectId(projectName, ownerUsername)
	if !errors.Is(sql.ErrNoRows, err) {
		return 0, fmt.Errorf("project already exists")
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

func (j JamHubDb) AddCollaborator(projectId uint64, collabUsername string) error {
	_, err := j.db.Exec("INSERT OR IGNORE INTO collaborators(project_id, username) VALUES(?, ?)", projectId, collabUsername)
	return err
}

func (j JamHubDb) ListCollaborators(projectId uint64) ([]string, error) {
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

func (j JamHubDb) HasCollaborator(projectId uint64, collaborator string) bool {
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

func (j JamHubDb) DeleteProject(projectName string, ownerUsername string) (uint64, error) {
	_, err := j.GetProjectId(projectName, ownerUsername)
	if errors.Is(sql.ErrNoRows, err) {
		return 0, fmt.Errorf("project does not exist")
	}

	res, err := j.db.Exec("DELETE FROM projects WHERE name = ? AND owner_username = ?", projectName, ownerUsername)
	if err != nil {
		return 0, err
	}

	var id int64
	if id, err = res.LastInsertId(); err != nil {
		return 0, err
	}

	return uint64(id), nil
}

func (j JamHubDb) GetUserId(username string) (string, error) {
	row := j.db.QueryRow("SELECT user_id FROM users WHERE username = ?", username)
	if row.Err() != nil {
		return "", row.Err()
	}

	var userId string
	err := row.Scan(&userId)
	return userId, err
}

func (j JamHubDb) GetUsername(user_id string) (string, error) {
	row := j.db.QueryRow("SELECT username FROM users WHERE user_id = ?", user_id)
	if row.Err() != nil {
		return "", row.Err()
	}

	var userId string
	err := row.Scan(&userId)
	return userId, err
}

func (j JamHubDb) GetProjectOwnerUsername(projectId uint64) (string, error) {
	row := j.db.QueryRow("SELECT owner_username FROM projects WHERE rowid = ?", projectId)
	if row.Err() != nil {
		return "", row.Err()
	}

	var ownerUsername string
	err := row.Scan(&ownerUsername)
	return ownerUsername, err
}

func (j JamHubDb) GetProjectId(projectName string, ownerUsername string) (uint64, error) {
	row := j.db.QueryRow("SELECT rowid FROM projects WHERE name = ? AND owner_username = ?", projectName, ownerUsername)
	if row.Err() != nil {
		return 0, row.Err()
	}

	var id uint64
	err := row.Scan(&id)
	return id, err
}

func (j JamHubDb) GetProjectName(id uint64, ownerUsername string) (string, error) {
	row := j.db.QueryRow("SELECT name FROM projects WHERE rowid = ? AND owner_username = ?", id, ownerUsername)
	if row.Err() != nil {
		return "", row.Err()
	}

	var name string
	err := row.Scan(&name)
	return name, err
}

func (j JamHubDb) ListProjectsOwned(ownerUsername string) ([]Project, error) {
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

func (j JamHubDb) ListProjectsAsCollaborator(username string) ([]Project, error) {
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

func (j JamHubDb) CreateUser(username, userId string) error {
	_, err := j.db.Exec("INSERT OR IGNORE INTO users(username, user_id) VALUES (?, ?)", username, userId)
	return err
}

func (j JamHubDb) Username(userId string) (string, error) {
	row := j.db.QueryRow("SELECT username FROM users WHERE user_id = ?", userId)
	if row.Err() != nil {
		return "", row.Err()
	}

	var username string
	err := row.Scan(&username)
	return username, err
}

func (j JamHubDb) UserId(username string) (string, error) {
	row := j.db.QueryRow("SELECT user_id FROM users WHERE username = ?", username)
	if row.Err() != nil {
		return "", row.Err()
	}

	var userId string
	err := row.Scan(&username)
	return userId, err
}
