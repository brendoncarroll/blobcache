package blobnet

import (
	"context"
	"testing"

	"github.com/blobcache/blobcache/pkg/bcstate"
	"github.com/blobcache/blobcache/pkg/blobnet/peers"
	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/p/dynmux"
	"github.com/brendoncarroll/go-p2p/p2ptest"
	"github.com/brendoncarroll/go-p2p/s/memswarm"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
)

func TestBlobnet(t *testing.T) {
	const N = 10
	realm := memswarm.NewRealm()
	swarms := make([]p2p.SecureAskSwarm, N)
	for i := range swarms {
		swarms[i] = realm.NewSwarmWithKey(p2ptest.NewTestKey(t, i))
	}

	adjList := p2ptest.Chain(p2ptest.CastSlice(swarms))

	bns := make([]*Blobnet, N)
	for i := range swarms {
		peerStore := make(peers.MemPeerStore)
		for _, addr := range adjList[i] {
			pubKey, err := swarms[i].LookupPublicKey(context.TODO(), addr)
			require.NoError(t, err)
			id := p2p.NewPeerID(pubKey)
			peerStore.AddAddr(id, addr)
		}
		bns[i] = makeBlobnet(swarms[i], peerStore)
	}

	for i := range bns {
		bns[i].bootstrap(context.TODO())
	}
}

func makeBlobnet(s p2p.SecureAskSwarm, ps peers.PeerStore) *Blobnet {
	mux := dynmux.MultiplexSwarm(s)
	bn := NewBlobNet(Params{
		PeerStore: ps,
		Mux:       mux,
		DB:        &bcstate.MemDB{},
		Local:     bcstate.BlobAdapter(&bcstate.MemKV{Capacity: 100}),
		Clock:     clockwork.NewRealClock(),
	})
	return bn
}
