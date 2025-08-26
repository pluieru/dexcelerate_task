package store

import (
	"context"
	"dexc_conf/internal/models"
	"time"
)

type Bucket struct {
	Start   time.Time `json:"start"`    // start of the bucket
	VolUSD  float64   `json:"vol_usd"`  // volume in USD
	TxCount int64     `json:"tx_count"` // transaction count
}

func (b *Bucket) Add(volUSD float64, txCount int64) {
	b.VolUSD += volUSD
	b.TxCount += txCount
}

func (b Bucket) IsEmpty() bool {
	return b.VolUSD == 0 && b.TxCount == 0
}

type StateStore interface {
	Add(ctx context.Context, token string, window models.Window, bucket Bucket) error

	LoadRange(ctx context.Context, token string, window models.Window, from, to time.Time) ([]Bucket, error)

	GetTokens(ctx context.Context) ([]string, error)

	Snapshot(ctx context.Context, snap Snapshot) error

	LoadSnapshot(ctx context.Context) (Snapshot, error)

	Close() error
}
