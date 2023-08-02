package jamgrpc

import (
	"context"
	"errors"

	"github.com/zdgeier/jam/gen/pb"
	"github.com/zdgeier/jam/pkg/jamgrpc/serverauth"
	"github.com/zdgeier/jam/pkg/jamstores/db"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s JamHub) GetProjectName(ctx context.Context, in *pb.GetProjectNameRequest) (*pb.GetProjectNameResponse, error) {
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

	return &pb.GetProjectNameResponse{
		ProjectName: projectName,
	}, nil
}

func (s JamHub) AddProject(ctx context.Context, in *pb.AddProjectRequest) (*pb.AddProjectResponse, error) {
	id, err := serverauth.ParseIdFromCtx(ctx)
	if err != nil {
		return nil, err
	}

	username, err := s.db.GetUsername(id)
	if err != nil {
		return nil, err
	}

	projectId, err := s.db.AddProject(in.GetProjectName(), username)
	if errors.Is(err, db.ProjectAlreadyExists) {
		return nil, status.Error(codes.AlreadyExists, db.ProjectAlreadyExists.Error())
	}

	err = s.opdatastorecommit.AddProject(username, projectId)
	if err != nil {
		return nil, err
	}

	err = s.oplocstorecommit.AddProject(username, projectId)
	if err != nil {
		return nil, err
	}

	return &pb.AddProjectResponse{
		ProjectId:     projectId,
		OwnerUsername: username,
	}, nil
}

func (s JamHub) GetCollaborators(ctx context.Context, in *pb.GetCollaboratorsRequest) (*pb.GetCollaboratorsResponse, error) {
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

	return &pb.GetCollaboratorsResponse{
		Usernames: usernames,
	}, nil
}

func (s JamHub) AddCollaborator(ctx context.Context, in *pb.AddCollaboratorRequest) (*pb.AddCollaboratorResponse, error) {
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

	return &pb.AddCollaboratorResponse{}, nil
}

func (s JamHub) ListUserProjects(ctx context.Context, in *pb.ListUserProjectsRequest) (*pb.ListUserProjectsResponse, error) {
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

	projectsPb := make([]*pb.ListUserProjectsResponse_Project, 0, len(projects)+len(collabProjects))
	for _, p := range projects {
		projectsPb = append(projectsPb, &pb.ListUserProjectsResponse_Project{OwnerUsername: p.OwnerUsername, Name: p.Name, Id: p.Id})
	}
	for _, p := range collabProjects {
		projectsPb = append(projectsPb, &pb.ListUserProjectsResponse_Project{OwnerUsername: p.OwnerUsername, Name: p.Name, Id: p.Id})
	}

	return &pb.ListUserProjectsResponse{Projects: projectsPb}, nil
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

// func (s JamHub) ProjectOwner(ownerUsername string, projectName string) (bool, error) {
// 	projectId, err := s.db.GetProjectId(projectName, ownerUsername)
// 	if err != nil {
// 		return false, err
// 	}

// 	projectOwnerUsername, err := s.db.GetProjectOwnerUsername(projectId)
// 	if err != nil {
// 		return false, err
// 	}

// 	if ownerUsername == projectOwnerUsername {
// 		return true, nil
// 	}

// 	return false, nil
// }

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

func (s JamHub) GetProjectId(ctx context.Context, in *pb.GetProjectIdRequest) (*pb.GetProjectIdResponse, error) {
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

	return &pb.GetProjectIdResponse{ProjectId: projectId}, nil
}

func (s JamHub) DeleteProject(ctx context.Context, in *pb.DeleteProjectRequest) (*pb.DeleteProjectResponse, error) {
	id, err := serverauth.ParseIdFromCtx(ctx)
	if err != nil {
		return nil, err
	}

	projectName := in.GetProjectName()
	if in.GetProjectId() != 0 {
		projectName, err = s.db.GetProjectName(in.GetProjectId(), id)
		if err != nil {
			return nil, err
		}
	}

	projectId, err := s.db.DeleteProject(projectName, id)
	if err != nil {
		return nil, err
	}
	err = s.changestore.DeleteProject(projectId, id)
	if err != nil {
		return nil, err
	}
	err = s.oplocstoreworkspace.DeleteProject(id, projectId)
	if err != nil {
		return nil, err
	}
	err = s.oplocstorecommit.DeleteProject(id, projectId)
	if err != nil {
		return nil, err
	}
	err = s.opdatastoreworkspace.DeleteProject(id, projectId)
	if err != nil {
		return nil, err
	}
	err = s.opdatastorecommit.DeleteProject(id, projectId)
	if err != nil {
		return nil, err
	}
	return &pb.DeleteProjectResponse{
		ProjectId:   projectId,
		ProjectName: projectName,
	}, nil
}
