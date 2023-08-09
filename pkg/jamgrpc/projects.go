package jamgrpc

import (
	"context"
	"errors"

	"github.com/zdgeier/jam/gen/jampb"
	"github.com/zdgeier/jam/pkg/jamgrpc/serverauth"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s JamHub) GetProjectName(ctx context.Context, in *jampb.GetProjectNameRequest) (*jampb.GetProjectNameResponse, error) {
	userId, err := serverauth.ParseIdFromCtx(ctx)
	if err != nil {
		return nil, err
	}

	currentUsername, err := s.db.GetUsername(userId)
	if err != nil {
		return nil, err
	}

	accessible, err := s.ProjectIdAccessible(in.GetOwnerUsername(), in.GetProjectId(), currentUsername)
	if err != nil {
		return nil, err
	}

	if !accessible {
		return nil, errors.New("not an owner or collaborator of this project")
	}

	projectName, err := s.db.GetProjectName(in.GetProjectId(), currentUsername)
	if err != nil {
		return nil, err
	}

	return &jampb.GetProjectNameResponse{
		ProjectName: projectName,
	}, nil
}

func (s JamHub) AddProject(ctx context.Context, in *jampb.AddProjectRequest) (*jampb.AddProjectResponse, error) {
	id, err := serverauth.ParseIdFromCtx(ctx)
	if err != nil {
		return nil, err
	}

	username, err := s.db.GetUsername(id)
	if err != nil {
		return nil, err
	}

	projectId, err := s.db.AddProject(in.GetProjectName(), username)
	if err != nil {
		return nil, status.Error(codes.AlreadyExists, "project name already exists")
	}

	return &jampb.AddProjectResponse{
		ProjectId:     projectId,
		OwnerUsername: username,
	}, nil
}

func (s JamHub) GetCollaborators(ctx context.Context, in *jampb.GetCollaboratorsRequest) (*jampb.GetCollaboratorsResponse, error) {
	userId, err := serverauth.ParseIdFromCtx(ctx)
	if err != nil {
		return nil, err
	}

	if in.GetOwnerUsername() == "" {
		return nil, errors.New("must provide owner username")
	}

	currentUsername, err := s.db.GetUsername(userId)
	if err != nil {
		return nil, err
	}

	accessible, err := s.ProjectIdAccessible(in.GetOwnerUsername(), in.GetProjectId(), currentUsername)
	if err != nil {
		return nil, err
	}

	if !accessible {
		return nil, errors.New("not an owner or collaborator of this project")
	}

	usernames, err := s.db.ListCollaborators(in.GetProjectId())
	if err != nil {
		return nil, err
	}

	return &jampb.GetCollaboratorsResponse{
		Usernames: usernames,
	}, nil
}

func (s JamHub) AddCollaborator(ctx context.Context, in *jampb.AddCollaboratorRequest) (*jampb.AddCollaboratorResponse, error) {
	userId, err := serverauth.ParseIdFromCtx(ctx)
	if err != nil {
		return nil, err
	}

	if in.GetOwnerUsername() == "" {
		return nil, errors.New("must provide owner id")
	}

	currentUsername, err := s.db.GetUsername(userId)
	if err != nil {
		return nil, err
	}

	accessible, err := s.ProjectIdAccessible(in.GetOwnerUsername(), in.GetProjectId(), currentUsername)
	if err != nil {
		return nil, err
	}

	if !accessible {
		return nil, errors.New("not an owner or collaborator of this project")
	}

	err = s.db.AddCollaborator(in.GetProjectId(), in.GetUsername())
	if err != nil {
		return nil, err
	}

	return &jampb.AddCollaboratorResponse{}, nil
}

func (s JamHub) ListUserProjects(ctx context.Context, in *jampb.ListUserProjectsRequest) (*jampb.ListUserProjectsResponse, error) {
	id, err := serverauth.ParseIdFromCtx(ctx)
	if err != nil {
		return nil, err
	}

	username, err := s.db.GetUsername(id)
	if err != nil {
		return nil, err
	}

	projects, err := s.db.ListProjectsOwned(username)
	if err != nil {
		return nil, err
	}

	collabProjects, err := s.db.ListProjectsAsCollaborator(username)
	if err != nil {
		return nil, err
	}

	projectsPb := make([]*jampb.ListUserProjectsResponse_Project, 0, len(projects)+len(collabProjects))
	for _, p := range projects {
		projectsPb = append(projectsPb, &jampb.ListUserProjectsResponse_Project{OwnerUsername: p.OwnerUsername, Name: p.Name, Id: p.Id})
	}
	for _, p := range collabProjects {
		projectsPb = append(projectsPb, &jampb.ListUserProjectsResponse_Project{OwnerUsername: p.OwnerUsername, Name: p.Name, Id: p.Id})
	}

	return &jampb.ListUserProjectsResponse{Projects: projectsPb}, nil
}

func (s JamHub) ProjectIdOwner(owner string, projectId uint64) (bool, error) {
	projectOwner, err := s.db.GetProjectOwnerUsername(projectId)
	if err != nil {
		return false, err
	}

	if owner == projectOwner {
		return true, nil
	}

	return false, nil
}

func (s JamHub) ProjectIdAccessible(ownerUsername string, projectId uint64, currentUsername string) (bool, error) {
	projectOwnerUsername, err := s.db.GetProjectOwnerUsername(projectId)
	if err != nil {
		return false, err
	}

	if ownerUsername == projectOwnerUsername && projectOwnerUsername == currentUsername {
		return true, nil
	}

	if s.db.HasCollaborator(projectId, currentUsername) {
		return true, nil
	}

	return false, nil
}

func (s JamHub) ProjectAccessible(owner string, projectName string, currentUsername string) (bool, error) {
	projectId, err := s.db.GetProjectId(projectName, owner)
	if err != nil {
		return false, err
	}

	projectOwner, err := s.db.GetProjectOwnerUsername(projectId)
	if err != nil {
		return false, err
	}

	if owner == projectOwner && owner == currentUsername {
		return true, nil
	}

	if s.db.HasCollaborator(projectId, currentUsername) {
		return true, nil
	}

	return false, nil
}

func (s JamHub) GetProjectId(ctx context.Context, in *jampb.GetProjectIdRequest) (*jampb.GetProjectIdResponse, error) {
	userId, err := serverauth.ParseIdFromCtx(ctx)
	if err != nil {
		return nil, err
	}

	if in.GetOwnerUsername() == "" {
		return nil, errors.New("must provide owner id")
	}

	currentUsername, err := s.db.GetUsername(userId)
	if err != nil {
		return nil, err
	}

	canAccess, err := s.ProjectAccessible(in.GetOwnerUsername(), in.GetProjectName(), currentUsername)
	if err != nil {
		return nil, err
	} else if !canAccess {
		return nil, errors.New("cannot access project")
	}

	projectId, err := s.db.GetProjectId(in.GetProjectName(), in.GetOwnerUsername())
	if err != nil {
		return nil, err
	}

	return &jampb.GetProjectIdResponse{ProjectId: projectId}, nil
}
