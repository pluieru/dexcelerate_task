package models

import "time"

type Aggregate struct {
	VolUSD  float64 `json:"vol_usd"`
	TxCount int64   `json:"tx_count"`
}

func (a *Aggregate) Add(volUSD float64, txCount int64) {
	a.VolUSD += volUSD
	a.TxCount += txCount
}

func (a *Aggregate) Subtract(volUSD float64, txCount int64) {
	a.VolUSD -= volUSD
	a.TxCount -= txCount
	if a.VolUSD < 0 {
		a.VolUSD = 0
	}
	if a.TxCount < 0 {
		a.TxCount = 0
	}
}

type Aggregates struct {
	W5m  Aggregate `json:"w5m"`
	W1h  Aggregate `json:"w1h"`
	W24h Aggregate `json:"w24h"`
}

func (a Aggregates) GetByWindow(window Window) Aggregate {
	switch window {
	case Window5m:
		return a.W5m
	case Window1h:
		return a.W1h
	case Window24h:
		return a.W24h
	default:
		return Aggregate{}
	}
}

func (a *Aggregates) SetByWindow(window Window, agg Aggregate) {
	switch window {
	case Window5m:
		a.W5m = agg
	case Window1h:
		a.W1h = agg
	case Window24h:
		a.W24h = agg
	}
}

type TokenStats struct {
	Token string    `json:"token"`
	W5m   Aggregate `json:"w5m"`
	W1h   Aggregate `json:"w1h"`
	W24h  Aggregate `json:"w24h"`
	TS    time.Time `json:"ts"`
}
