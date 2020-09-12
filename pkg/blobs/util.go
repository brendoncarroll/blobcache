package blobs

import (
	"context"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

func Copy(ctx context.Context, src Getter, dst Poster, id ID) error {
	return src.GetF(ctx, id, func(data []byte) error {
		id2, err := dst.Post(ctx, data)
		if err != nil {
			return err
		}
		if !id.Equals(id2) {
			return errors.Errorf("src id: %v, dst id: %v", id, id2)
		}
		return nil
	})
}

func GC(ctx context.Context, target Store, prefix []byte, keep func(ctx context.Context, id ID) (bool, error)) error {
	toDelete := make(chan ID, 32)
	eg := errgroup.Group{}
	eg.Go(func() error {
		defer close(toDelete)
		return ListPrefix(ctx, target, prefix, func(id ID) error {
			exists, err := keep(ctx, id)
			if err != nil {
				return err
			}
			if !exists {
				toDelete <- id
			}
			return nil
		})
	})
	eg.Go(func() error {
		for id := range toDelete {
			if err := target.Delete(ctx, id); err != nil {
				return err
			}
		}
		return nil
	})
	return eg.Wait()
}

func ForEach(ctx context.Context, s Lister, fn func(ID) error) error {
	return ListPrefix(ctx, s, []byte{}, fn)
}

func ListPrefix(ctx context.Context, s Lister, prefix []byte, fn func(ID) error) error {
	ids := make([]ID, 1<<10)
	n, err := s.List(ctx, prefix, ids)
	switch {
	case err == ErrTooMany:
		for i := 0; i < 256; i++ {
			prefix2 := append(prefix, byte(i))
			if err := ListPrefix(ctx, s, prefix2, fn); err != nil {
				return err
			}
		}
		return nil
	case err != nil:
		return err
	default:
		for _, id := range ids[:n] {
			if err := fn(id); err != nil {
				return err
			}
		}
		return nil
	}
}
