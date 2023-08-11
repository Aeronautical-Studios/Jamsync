package jamgrpc

import (
	"context"

	"github.com/zdgeier/jam/gen/jampb"
	"github.com/zdgeier/jam/pkg/jamgrpc/serverauth"
)

func (s JamHub) CreateUser(ctx context.Context, in *jampb.CreateUserRequest) (*jampb.CreateUserResponse, error) {
	id, err := serverauth.ParseIdFromCtx(ctx)
	if err != nil {
		return nil, err
	}

	err = s.db.CreateUser(in.GetUsername(), id)
	if err != nil {
		return nil, err
	}
	return &jampb.CreateUserResponse{}, nil
}

func (s JamHub) CurrentUser(ctx context.Context, in *jampb.CurrentUserRequest) (*jampb.CurrentUserResponse, error) {
	id, err := serverauth.ParseIdFromCtx(ctx)
	if err != nil {
		return nil, err
	}

	username, err := s.db.Username(id)
	if err != nil {
		return nil, err
	}

	return &jampb.CurrentUserResponse{Username: username}, nil
}
