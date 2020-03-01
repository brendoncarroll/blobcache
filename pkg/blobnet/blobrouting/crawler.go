package blobrouting

import (
	"context"
	"time"

	"github.com/brendoncarroll/blobcache/pkg/bitstrings"
	"github.com/brendoncarroll/blobcache/pkg/blobnet/peerrouting"
	"github.com/brendoncarroll/blobcache/pkg/blobs"
	"github.com/brendoncarroll/blobcache/pkg/tries"
	"github.com/brendoncarroll/go-p2p"
	log "github.com/sirupsen/logrus"
)

type ShardID struct {
	PeerID p2p.PeerID
	Prefix string
}

type Crawler struct {
	peerRouter *peerrouting.Router
	blobRouter *Router
	routeTable *KadRT
	peerSwarm  PeerSwarm

	shards map[ShardID]blobs.ID
}

func newCrawler(peerRouter *peerrouting.Router, blobRouter *Router, peerSwarm PeerSwarm) *Crawler {
	return &Crawler{
		peerRouter: peerRouter,
		blobRouter: blobRouter,
		peerSwarm:  peerSwarm,
		routeTable: blobRouter.routeTable,
		shards:     map[ShardID]blobs.ID{},
	}
}

func (c *Crawler) run(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.crawl(ctx); err != nil {
				log.Error(err)
			}
		}
	}
}

func (c *Crawler) crawl(ctx context.Context) error {
	peerIDs := []p2p.PeerID{}
	peerIDs = append(peerIDs, c.peerRouter.OneHop()...)
	peerIDs = append(peerIDs, c.peerRouter.MultiHop()...)

	for _, peerID := range peerIDs {
		bitstr := c.routeTable.WouldAccept()
		for _, prefix := range bitstr.EnumBytePrefixes() {
			x := bitstrings.FromBytes(len(prefix)*8, prefix)
			if !c.routeTable.WouldAccept().HasPrefix(&x) {
				continue
			}
			err := c.indexPeer(ctx, peerID, prefix)
			if err == ErrShouldEvictThis {
				continue
			}
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Crawler) indexPeer(ctx context.Context, peerID p2p.PeerID, prefix []byte) error {
	shardID := ShardID{peerID, string(prefix)}
	rt, nextHop := c.peerRouter.Lookup(peerID)
	if rt == nil {
		delete(c.shards, shardID)
		return peerrouting.ErrNoRouteToPeer
	}

	req := &ListBlobsReq{
		RoutingTag: rt,
		Prefix:     prefix,
	}
	res, err := c.blobRouter.request(ctx, nextHop, req)
	if err != nil {
		delete(c.shards, ShardID{peerID, string(prefix)})
		return err
	}

	// Sharded below this point
	if len(res.TrieHash) < 1 || len(res.TrieData) < 1 {
		delete(c.shards, shardID)
		for i := 0; i < 256; i++ {
			prefix2 := append(prefix, byte(i))
			if err := c.indexPeer(ctx, peerID, prefix2); err != nil {
				return err
			}
		}
		return nil
	}

	// parse trie; can't resolve any of the children without a store, but we don't need to here.
	t, err := tries.FromBytes(nil, res.TrieData)
	if err != nil {
		return err
	}
	id := blobs.Hash(res.TrieData)

	// parent, need to recurse
	if t.IsParent() {
		for i := 0; i < 256; i++ {
			id := t.GetChildRef(byte(i))
			prefix2 := append(prefix, byte(i))
			shardID2 := ShardID{peerID, string(prefix2)}
			if id2, exists := c.shards[shardID2]; exists && id.Equals(id2) {
				continue
			}
			if err := c.indexPeer(ctx, peerID, prefix2); err != nil {
				return err
			}
		}
		c.shards[shardID] = id
		return nil
	}

	// child
	for _, pair := range t.ListEntries() {
		blobID, peerID := splitKey(pair.Key)
		c.routeTable.Put(ctx, blobID, peerID)
	}
	c.shards[shardID] = id
	return nil
}

func splitKey(x []byte) (blobs.ID, p2p.PeerID) {
	l := len(x)
	blobID := blobs.ID{}
	peerID := p2p.PeerID{}
	copy(blobID[:], x[l/2:])
	copy(peerID[:], x[:l/2])
	return blobID, peerID
}

func makeKey(blobID blobs.ID, peerID p2p.PeerID) []byte {
	return append(blobID[:], peerID[:]...)
}
