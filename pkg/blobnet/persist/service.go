package persist

import (
	"context"

	"github.com/blobcache/blobcache/pkg/bcstate"
	"github.com/blobcache/blobcache/pkg/blobnet/bcproto"
	"github.com/blobcache/blobcache/pkg/blobnet/onehoppull"
	"github.com/blobcache/blobcache/pkg/blobs"
	"github.com/blobcache/blobcache/pkg/tries"
	"github.com/brendoncarroll/go-p2p"
	"golang.org/x/sync/errgroup"
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
	puller     *onehoppull.OneHopPull

	promisesFromUs bcstate.KV
	promisesToUs   bcstate.KV
}

func NewService(params Params) *Service {
	return &Service{}
}

func (s *Service) Persist(ctx context.Context, peerID p2p.PeerID) (bcproto.Promise, error) {
	return nil
}

func (s *Service) pullTrie(ctx context.Context, peerID p2p.PeerID, root blobs.ID) error {
	data, err := s.puller.Pull(ctx, peerID, root)
	if err != nil {
		return err
	}
	trie, err := tries.FromBytes(blobs.Void{}, data)
	if err != nil {
		return err
	}
	if trie.IsParent() {
		for i := 0; i < 256; i++ {
			childID := trie.GetChildRef(byte(i))
			if childID.Equals(blobs.ID{}) {
				continue
			}
			if err := pullTrie(ctx, peerID, childID); err != nil {
				return err
			}
		}
	} else {
		group, ctx := errgroup.WithContext(ctx)
		for _, pair := range trie.ListEntries() {
			pair := pair
			group.Go(func() error {
				return s.pullBlob(ctx, peerID)
			})
		}
		if err := group.Wait(); err != nil {
			return err
		}
	}
	_, err := s.localStore.Post(ctx, data)
	return err
}

func (s *Service) pullBlob(ctx context.Context, peerID p2p.PeerID, blobID blobs.ID) error {
	if exists, err := s.localStore.Exists(ctx, blobID); err != nil {
		return err
	}
	if exists {
		return nil
	}
	data, err := s.puller.Pull(ctx, peerID, blobID)
	if err != nil {
		return err
	}
	_, err := s.localStore.Post(ctx, data)
	return err
}

func (s *Service) shouldKeep(ctx context.Context, blobID) (bool, error) {
	exists, err := s.localSet.Exists(ctx, blobID)
	if err != nil {
		return false, err
	}
	if exists {
		return true
	}
}

func (s *Service) GC(ctx context.Context) error {
	return blobs.ForEach(ctx, s.localStore, func(id blobs.ID) error {
		yes, err := s.shouldKeep(ctx, id)
		if err != nil {
			return err
		}
		if !yes {
			return s.localStore.Delete(ctx, id)
		}
		return nil
	})
}
