package persist

import (
	"context"

	"github.com/brendoncarroll/blobcache/pkg/bcstate"
	"github.com/brendoncarroll/blobcache/pkg/blobs"
	"github.com/brendoncarroll/blobcache/pkg/tries"
	"github.com/brendoncarroll/go-p2p"
)

type Params struct {
	LocalSet   blobs.Set
	LocalStore blobs.Store
	LocalID    p2p.PeerID
	DB         bcstate.DB
	Swarm      p2p.Swarm
}

type Service struct {
	localSet   blobs.Set
	localStore blobs.Store

	promisesFromUs bcstate.KV
	promisesToUs   bcstate.KV
}

func NewService(params Params) *Service {
	return &Service{}
}

func (s *Service) Persist(ctx context.Context, t tries.Trie) error {
	return nil
}

func (s *Service) GC(ctx context.Context) error {
	err := blobs.ForEach(ctx, s.localStore, func(id blobs.ID) error {
		exists, err := s.localSet.Exists(ctx, id)
		if err != nil {
			return err
		}
		if !exists {
			return s.localStore.Delete(ctx, id)
		}
		return nil
	})
	return err
}
