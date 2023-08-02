package jamgrpc

import (
	"context"

	"github.com/zdgeier/jam/gen/pb"
	"github.com/zdgeier/jam/pkg/jamgrpc/serverauth"
)

func (s JamHub) CreateUser(ctx context.Context, in *pb.CreateUserRequest) (*pb.CreateUserResponse, error) {
	id, err := serverauth.ParseIdFromCtx(ctx)
	if err != nil {
		return nil, err
	}

	err = s.db.CreateUser(in.GetUsername(), id)
	if err != nil {
		return nil, err
	}
	return &pb.CreateUserResponse{}, nil
}

func (s JamHub) CurrentUser(ctx context.Context, in *pb.CurrentUserRequest) (*pb.CurrentUserResponse, error) {
	id, err := serverauth.ParseIdFromCtx(ctx)
	if err != nil {
		return nil, err
	}

	username, err := s.db.Username(id)
	if err != nil {
		return nil, err
	}

	return &pb.CurrentUserResponse{Username: username}, nil
}

func (s JamHub) Ping(ctx context.Context, in *pb.PingRequest) (*pb.Pong, error) {
	return &pb.Pong{}, nil
}
