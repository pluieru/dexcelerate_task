package store

import (
	"context"
	"time"
)

type OffsetStore interface {
	SaveWatermark(ctx context.Context, ts time.Time) error

	LoadWatermark(ctx context.Context) (time.Time, error)

	Close() error
}
