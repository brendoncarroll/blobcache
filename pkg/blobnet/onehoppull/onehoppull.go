package onehoppull

import (
	"context"
	"io"
	"sync"

	"github.com/blobcache/blobcache/pkg/blobnet/peers"
	"github.com/blobcache/blobcache/pkg/blobs"
	"github.com/brendoncarroll/go-p2p"
	"github.com/jonboulle/clockwork"
	log "github.com/sirupsen/logrus"
)

type Params struct {
	PeerSwarm peers.PeerSwarm
	Local     blobs.Getter
	Clock     clockwork.Clock
}

type OneHopPull struct {
	peerSwarm peers.PeerSwarm
	local     blobs.Getter

	mu    sync.RWMutex
	seq   int
	rules map[int]func(context.Context, p2p.PeerID, blobs.ID) bool
}

func NewOneHopPull(params Params) *OneHopPull {
	ohp := &OneHopPull{
		peerSwarm: params.PeerSwarm,
		local:     params.Local,

		rules: make(map[int]func(context.Context, p2p.PeerID, blobs.ID) bool),
	}
	ohp.peerSwarm.OnAsk(ohp.handleAsk)
	return ohp
}

func (ohp *OneHopPull) handleAsk(ctx context.Context, msg *p2p.Message, w io.Writer) {
	id := blobs.ID{}
	copy(id[:], msg.Payload)

	if !ohp.isAllowed(ctx, msg.Src.(p2p.PeerID), id) {
		return
	}

	err := ohp.local.GetF(ctx, id, func(data []byte) error {
		_, err := w.Write(data)
		return err
	})
	if err != nil {
		log.Error(err)
	}
}

func (ohp *OneHopPull) Pull(ctx context.Context, pid p2p.PeerID, bid blobs.ID) ([]byte, error) {
	return ohp.peerSwarm.AskPeer(ctx, pid, bid[:])
}

func (ohp *OneHopPull) AddRule(peerID p2p.PeerID, f func(context.Context, blobs.ID) bool) int {
	ohp.mu.Lock()
	defer ohp.mu.Unlock()
	x := ohp.seq
	ohp.seq++

	ohp.rules[x] = func(ctx context.Context, xpeer p2p.PeerID, xblob blobs.ID) bool {
		return xpeer.Equals(peerID) && f(ctx, xblob)
	}
	return x
}

func (ohp *OneHopPull) DropRule(x int) {
	ohp.mu.Lock()
	defer ohp.mu.Unlock()
	delete(ohp.rules, x)
}

func (ohp *OneHopPull) isAllowed(ctx context.Context, peerID p2p.PeerID, blobID blobs.ID) bool {
	ohp.mu.RLock()
	defer ohp.mu.RUnlock()
	for _, f := range ohp.rules {
		if f(ctx, peerID, blobID) {
			return true
		}
	}
	return false
}

func (ohp *OneHopPull) Getter(peerID p2p.PeerID) blobs.Getter {
	return &getter{peerID: peerID, ohp: ohp}
}

type getter struct {
	peerID p2p.PeerID
	ohp    *OneHopPull
}

func (g *getter) GetF(ctx context.Context, id blobs.ID, fn func(data []byte) error) error {
	data, err := g.ohp.Pull(ctx, g.peerID, id)
	if err != nil {
		return err
	}
	return fn(data)
}

func (g *getter) Exists(ctx context.Context, id blobs.ID) (bool, error) {
	err := g.GetF(ctx, id, func([]byte) error { return nil })
	if err != nil {
		if err == blobs.ErrNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
