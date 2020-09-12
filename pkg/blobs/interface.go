package blobs

import (
	"context"
	"errors"
)

type Store interface {
	GetPostDelete
	Lister
}

type GetPostDelete interface {
	Getter
	Poster
	Deleter
}

type Getter interface {
	GetF(context.Context, ID, func(Blob) error) error
	Exists
}

type Poster interface {
	Post(context.Context, Blob) (ID, error)
}

type Deleter interface {
	Delete(context.Context, ID) error
}

type Exists interface {
	Exists(context.Context, ID) (bool, error)
}

type Lister interface {
	// List fills ids with all the IDs under that prefix
	List(ctx context.Context, prefix []byte, ids []ID) (n int, err error)
}

type Set interface {
	Exists
	Lister
}

var (
	ErrTooMany  = errors.New("prefix would take up more space than buffer")
	ErrNotFound = errors.New("blob not found")
)
