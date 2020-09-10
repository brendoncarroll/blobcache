package persist

import (
	"github.com/brendoncarroll/blobcache/pkg/blobs"
	"github.com/brendoncarroll/go-p2p"
)

type Plan struct {
	Roots map[p2p.PeerID]blobs.ID
}
