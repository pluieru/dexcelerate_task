package models

import (
	"fmt"
	"time"
)

type Side string

const (
	SideBuy  Side = "buy"
	SideSell Side = "sell"
)

type Swap struct {
	ID       string    `json:"id"`
	Who      string    `json:"who"`
	Token    string    `json:"token"`
	Amount   float64   `json:"amount"`
	USD      float64   `json:"usd"`
	Side     Side      `json:"side"`
	TS       time.Time `json:"ts"`
	TxHash   string    `json:"tx_hash"`
	LogIndex int       `json:"log_index"`
}

func (s Swap) Key() string {
	if s.ID != "" {
		return s.ID
	}
	return fmt.Sprintf("%s:%d", s.TxHash, s.LogIndex)
}

func (s Swap) IsValid() bool {
	return s.Who != "" &&
		s.Token != "" &&
		s.Amount > 0 &&
		s.USD > 0 &&
		!s.TS.IsZero() &&
		s.TxHash != "" &&
		s.LogIndex >= 0 &&
		(s.Side == SideBuy || s.Side == SideSell)
}
