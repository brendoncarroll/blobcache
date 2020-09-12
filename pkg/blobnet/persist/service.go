package persist

import (
	"context"
	"sync"

	"github.com/blobcache/blobcache/pkg/bcstate"
	"github.com/blobcache/blobcache/pkg/blobnet/bcproto"
	"github.com/blobcache/blobcache/pkg/blobnet/onehoppull"
	"github.com/blobcache/blobcache/pkg/blobs"
	"github.com/blobcache/blobcache/pkg/tries"
	"github.com/brendoncarroll/go-p2p"
)

type Params struct {
	LocalSet                 blobs.Set
	DataStore, MetadataStore blobs.Store
	LocalID                  p2p.PeerID
	DB                       bcstate.DB
	Swarm                    p2p.Swarm
	OneHopPull               *onehoppull.OneHopPull
}

type Service struct {
	// what the local node wants to have persisted
	localSet blobs.Set

	dataStore     blobs.Store // where blobs are stored
	metadataStore blobs.Store // where tries are stored (as blobs)
	puller        *onehoppull.OneHopPull

	placerLock     sync.Mutex
	placer         *Placer
	plan           *Plan
	promisesFromUs bcstate.KV
	promisesToUs   bcstate.KV
	gcLock         sync.RWMutex
}

func NewService(params Params) *Service {
	return &Service{
		localSet: params.LocalSet,

		dataStore:     params.DataStore,
		metadataStore: params.MetadataStore,

		puller: params.OneHopPull,
	}
}

func (s *Service) run(ctx context.Context) {
	placer := &Placer{store: s.metadataStore}
}

func (s *Service) Persist(ctx context.Context, peerID p2p.PeerID) (bcproto.Promise, error) {
	return nil
}

func (s *Service) pullTrie(ctx context.Context, peerID p2p.PeerID, root blobs.ID) error {
	s.gcLock.RLock()
	src := s.puller.Getter(peerID)
	if err := tries.Sync(ctx, src, s.metadataStore, root); err != nil {
		s.gcLock.Unlock()
		return err
	}
	ts := &TrieSet{store: s.metadataStore}
	return blobs.ForEach(ctx, ts, func(id blobs.ID) error {
		return blobs.Copy(ctx, src, s.dataStore, id)
	})
	return nil
}

func (s *Service) GC(ctx context.Context) error {
	s.gcLock.Lock()
	defer s.gcLock.Unlock()
	var prefix []byte
	trieRoots := []blobs.ID{} // TODO: populate this
	// first GC the metadata store
	if err := tries.GC(ctx, s.metadataStore, trieRoots); err != nil {
		return err
	}
	union := Union{}
	for _, root := range trieRoots {
		union = append(union, &TrieSet{store: s.metadataStore, root: root})
	}
	return blobs.GC(ctx, s.dataStore, prefix, union.Exists)
}
