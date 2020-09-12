package persist

import (
	"context"

	"github.com/blobcache/blobcache/pkg/blobs"
	"github.com/blobcache/blobcache/pkg/tries"
)

type TrieSet struct {
	store blobs.Store
	root  blobs.ID
}

func (t *TrieSet) Exists(ctx context.Context, id blobs.ID) (bool, error) {
	_, err := tries.Get(ctx, t.store, t.root, id[:])
	if err != nil {
		if err == tries.ErrNotExist {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (t *TrieSet) List(ctx context.Context, prefix []byte, ids []blobs.ID) (int, error) {
	panic("not implemented")
}

func (t *TrieSet) Add(ctx context.Context, id blobs.ID) error {
	rootID, err := tries.Put(ctx, t.store, t.root, id[:], nil)
	if err != nil {
		return err
	}
	t.root = *rootID
	return nil
}

func (t *TrieSet) Flush(ctx context.Context) (blobs.ID, error) {
	return t.root, nil
}

type Union []blobs.Set

func (u Union) Exists(ctx context.Context, id blobs.ID) (bool, error) {
	var err error
	var ok bool
	for i := range u {
		ok, err = u[i].Exists(ctx, id)
		if err != nil {
			continue
		}
		if ok {
			return true, nil
		}
	}
	return false, err
}

func (u Union) List(ctx context.Context, prefix []byte, ids []blobs.ID) (int, error) {
	total := 0
	for i := range u {
		if total >= len(ids) {
			return -1, blobs.ErrTooMany
		}
		n, err := u[i].List(ctx, prefix, ids[total:])
		if err != nil {
			return -1, err
		}
		total += n
	}
	return total, nil
}
