package persist

import (
	"context"
	"sort"

	"github.com/blobcache/blobcache/pkg/bcstate"
	"github.com/blobcache/blobcache/pkg/blobs"
	"github.com/brendoncarroll/go-p2p"
)

type Plan struct {
	Placements map[p2p.PeerID]Placement
}

type Placement struct {
	Root  blobs.ID
	Count uint64
}

type Placer struct {
	store    blobs.Store
	replicas int

	compareCosts func(id blobs.ID, a, b p2p.PeerID) bool
	getCapacity  func(p2p.PeerID) uint64
}

func (p *Placer) AddIDToPlan(ctx context.Context, plan Plan, peers []p2p.PeerID, id blobs.ID) (*Plan, error) {
	loads := make(map[p2p.PeerID]uint64, len(plan.Placements))
	trieSets := map[p2p.PeerID]*TrieSet{}
	if err := p.place(ctx, loads, peers, id, func(peerID p2p.PeerID) error {
		if trieSets[peerID] == nil {
			trieSets[peerID] = &TrieSet{store: p.store}
		}
		return trieSets[peerID].Add(ctx, id)
	}); err != nil {
		return nil, err
	}
	for peerID, ts := range trieSets {
		root, err := ts.Flush(ctx)
		if err != nil {
			return nil, err
		}
		plan.Placements[peerID] = Placement{
			Root:  root,
			Count: plan.Placements[peerID].Count + 1,
		}
	}
	return &plan, nil
}

func (p *Placer) MakePlan(ctx context.Context, peers []p2p.PeerID, set blobs.Set) (*Plan, error) {
	loads := make(map[p2p.PeerID]uint64, len(peers))
	trieSets := map[p2p.PeerID]*TrieSet{}
	for _, peerID := range peers {
		trieSets[peerID] = &TrieSet{store: p.store}
	}

	if err := blobs.ForEach(ctx, set, func(id blobs.ID) error {
		return p.place(ctx, loads, peers, id, func(peerID p2p.PeerID) error {
			return trieSets[peerID].Add(ctx, id)
		})
	}); err != nil {
		return nil, err
	}

	plan := &Plan{
		Placements: make(map[p2p.PeerID]Placement),
	}
	for peerID, trieSet := range trieSets {
		root, err := trieSet.Flush(ctx)
		if err != nil {
			return nil, err
		}
		plan.Placements[peerID] = Placement{
			Root:  root,
			Count: loads[peerID],
		}
	}
	return plan, nil
}

func (p *Placer) place(ctx context.Context, loads map[p2p.PeerID]uint64, peers []p2p.PeerID, id blobs.ID, cb func(p2p.PeerID) error) error {
	sort.Slice(peers, func(i, j int) bool {
		return p.compareCosts(id, peers[i], peers[j])
	})
	replicas := 0
	for _, peerID := range peers {
		load := loads[peerID]
		capacity := p.getCapacity(peerID)
		if load >= capacity {
			continue
		}
		if err := cb(peerID); err != nil {
			return err
		}
		replicas++
		if replicas >= p.replicas {
			// all replicas placed, move on to next id
			return nil
		}
	}
	// Exhausted all options, we are full
	return bcstate.ErrFull
}
