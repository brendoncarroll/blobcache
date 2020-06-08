package onehoppull

import (
	"context"
	"io"
	"sync"

	"github.com/brendoncarroll/blobcache/pkg/blobnet/peers"
	"github.com/brendoncarroll/blobcache/pkg/blobs"
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

	mu      sync.RWMutex
	seq     int
	allowed map[int]func(context.Context, blobs.ID) bool
}

func NewOneHopPull(params Params) *OneHopPull {
	ohp := &OneHopPull{
		peerSwarm: params.PeerSwarm,
		local:     params.Local,

		allowed: make(map[int]func(context.Context, blobs.ID) bool),
	}
	ohp.peerSwarm.OnAsk(ohp.handleAsk)
	return ohp
}

func (ohp *OneHopPull) handleAsk(ctx context.Context, msg *p2p.Message, w io.Writer) {
	id := blobs.ID{}
	copy(id[:], msg.Payload)

	if !ohp.isAllowed(ctx, id) {
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

func (ohp *OneHopPull) AddSet(f func(context.Context, blobs.ID) bool) int {
	ohp.mu.Lock()
	defer ohp.mu.Unlock()
	x := ohp.seq
	ohp.seq++

	ohp.allowed[x] = f
	return x
}

func (ohp *OneHopPull) DropSet(x int) {
	ohp.mu.Lock()
	defer ohp.mu.Unlock()
	delete(ohp.allowed, x)
}

func (ohp *OneHopPull) isAllowed(ctx context.Context, id blobs.ID) bool {
	ohp.mu.RLock()
	defer ohp.mu.RUnlock()
	for _, f := range ohp.allowed {
		if f(ctx, id) {
			return true
		}
	}
	return false
}
