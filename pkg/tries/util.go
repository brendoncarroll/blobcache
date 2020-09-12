package tries

import (
	"context"

	"github.com/blobcache/blobcache/pkg/blobs"
)

func Sync(ctx context.Context, src blobs.Getter, dst blobs.Store, root blobs.ID) error {
	return walkRefs(ctx, src, root, func(id blobs.ID) error {
		return blobs.Copy(ctx, src, dst, id)
	})
}

func GC(ctx context.Context, target blobs.Store, roots []blobs.ID) error {
	reachable := map[blobs.ID]struct{}{}
	for _, root := range roots {
		if err := walkRefs(ctx, target, root, func(id blobs.ID) error {
			reachable[id] = struct{}{}
			return nil
		}); err != nil {
			return err
		}
	}
	return blobs.GC(ctx, target, nil, func(ctx context.Context, id blobs.ID) (bool, error) {
		_, exists := reachable[id]
		return exists, nil
	})
}

func walkRefs(ctx context.Context, s blobs.Getter, root blobs.ID, fn func(id blobs.ID) error) error {
	n, err := GetNode(ctx, s, root)
	if err != nil {
		return err
	}
	if IsParent(n) {
		for i := range n.Children {
			if len(n.Children) == 0 {
				continue
			}
			if err := walkRefs(ctx, s, blobs.IDFromBytes(n.Children[i]), fn); err != nil {
				return err
			}
		}
	}
	return fn(root)
}

func Merge(ctx context.Context, s blobs.Store, layers ...blobs.ID) (*blobs.ID, error) {
	if len(layers) == 0 {
		return PostNode(ctx, s, New())
	}
	if len(layers) == 1 {
		return &layers[0], nil
	}
	if len(layers) == 2 {
		left, err := GetNode(ctx, s, layers[0])
		if err != nil {
			return nil, err
		}
		right, err := GetNode(ctx, s, layers[0])
		if err != nil {
			return nil, err
		}
		switch {
		case !IsParent(left) && !IsParent(right):
			merged := &Node{}
			merged.Entries = append(merged.Entries, left.Entries...)
			merged.Entries = append(merged.Entries, right.Entries...)
			return PostNode(ctx, s, merged)
		case IsParent(left) && !IsParent(right):
			if right, err = Split(ctx, s, right); err != nil {
				return nil, err
			}
		case !IsParent(left) && IsParent(right):
			if left, err = Split(ctx, s, left); err != nil {
				return nil, err
			}
		}
		merged := &Node{Children: make([][]byte, 256)}
		for i := range merged.Children {
			var childID *blobs.ID
			var err error
			switch {
			case len(left.Children[i]) == blobs.IDSize && len(right.Children[i]) == blobs.IDSize:
				childID, err = Merge(ctx, s, blobs.IDFromBytes(left.Children[i]), blobs.IDFromBytes(right.Children[i]))
			case len(left.Children[i]) == blobs.IDSize:
				id := blobs.IDFromBytes(left.Children[i])
				childID = &id
			case len(right.Children[i]) == blobs.IDSize:
				id := blobs.IDFromBytes(right.Children[i])
				childID = &id
			}
			if err != nil {
				return nil, err
			}
			merged.Children[i] = childID[:]
		}
		return PostNode(ctx, s, merged)
	}
	l := len(layers)
	left, err := Merge(ctx, s, layers[:l/2]...)
	if err != nil {
		return nil, err
	}
	right, err := Merge(ctx, s, layers[l/2:]...)
	if err != nil {
		return nil, err
	}
	return Merge(ctx, s, *left, *right)
}
