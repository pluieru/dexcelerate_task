package models

import (
	"testing"
	"time"
)

func TestSwap_Key(t *testing.T) {
	tests := []struct {
		name string
		swap Swap
		want string
	}{
		{
			name: "swap with ID should use ID as key",
			swap: Swap{
				ID:       "custom_id_123",
				Who:      "0x123",
				Token:    "WETH",
				Amount:   100.0,
				USD:      1500.0,
				Side:     SideBuy,
				TS:       time.Now(),
				TxHash:   "0xabc",
				LogIndex: 1,
			},
			want: "custom_id_123",
		},
		{
			name: "swap without ID should generate stable key from fields",
			swap: Swap{
				Who:      "0x123ABC",
				Token:    "WETH",
				Amount:   100.0,
				USD:      1500.0,
				Side:     SideBuy,
				TS:       time.Now(),
				TxHash:   "0xABC123",
				LogIndex: 1,
			},
			want: "0xABC123:1",
		},
		{
			name: "different case should produce same key",
			swap: Swap{
				Who:      "0X123ABC",
				Token:    "weth",
				Amount:   100.0,
				USD:      1500.0,
				Side:     SideBuy,
				TS:       time.Now(),
				TxHash:   "0XABC123",
				LogIndex: 1,
			},
			want: "0XABC123:1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.swap.Key()
			if got != tt.want {
				t.Errorf("Swap.Key() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSwap_IsValid(t *testing.T) {
	validSwap := Swap{
		Who:      "0x123",
		Token:    "WETH",
		Amount:   100.0,
		USD:      1500.0,
		Side:     SideBuy,
		TS:       time.Now(),
		TxHash:   "0xabc",
		LogIndex: 0,
	}

	tests := []struct {
		name string
		swap Swap
		want bool
	}{
		{
			name: "valid swap",
			swap: validSwap,
			want: true,
		},
		{
			name: "empty who",
			swap: func() Swap {
				s := validSwap
				s.Who = ""
				return s
			}(),
			want: false,
		},
		{
			name: "empty token",
			swap: func() Swap {
				s := validSwap
				s.Token = ""
				return s
			}(),
			want: false,
		},
		{
			name: "zero amount",
			swap: func() Swap {
				s := validSwap
				s.Amount = 0
				return s
			}(),
			want: false,
		},
		{
			name: "negative amount",
			swap: func() Swap {
				s := validSwap
				s.Amount = -100
				return s
			}(),
			want: false,
		},
		{
			name: "zero USD",
			swap: func() Swap {
				s := validSwap
				s.USD = 0
				return s
			}(),
			want: false,
		},
		{
			name: "zero timestamp",
			swap: func() Swap {
				s := validSwap
				s.TS = time.Time{}
				return s
			}(),
			want: false,
		},
		{
			name: "empty tx hash",
			swap: func() Swap {
				s := validSwap
				s.TxHash = ""
				return s
			}(),
			want: false,
		},
		{
			name: "negative log index",
			swap: func() Swap {
				s := validSwap
				s.LogIndex = -1
				return s
			}(),
			want: false,
		},
		{
			name: "invalid side",
			swap: func() Swap {
				s := validSwap
				s.Side = "invalid"
				return s
			}(),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.swap.IsValid()
			if got != tt.want {
				t.Errorf("Swap.IsValid() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSwap_KeyStability(t *testing.T) {
	// Проверяем что ключ стабилен для одинаковых данных
	swap1 := Swap{
		Who:      "0x123",
		Token:    "WETH",
		Amount:   100.0,
		USD:      1500.0,
		Side:     SideBuy,
		TS:       time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
		TxHash:   "0xabc",
		LogIndex: 1,
	}

	swap2 := Swap{
		Who:      "0x123",
		Token:    "WETH",
		Amount:   100.0,
		USD:      1500.0,
		Side:     SideBuy,
		TS:       time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
		TxHash:   "0xabc",
		LogIndex: 1,
	}

	key1 := swap1.Key()
	key2 := swap2.Key()

	if key1 != key2 {
		t.Errorf("Keys should be identical for same swap data: %s != %s", key1, key2)
	}
}

func TestSwap_KeyUniqueness(t *testing.T) {
	// Проверяем что разные свопы дают разные ключи
	baseSwap := Swap{
		Who:      "0x123",
		Token:    "WETH",
		Amount:   100.0,
		USD:      1500.0,
		Side:     SideBuy,
		TS:       time.Now(),
		TxHash:   "0xabc",
		LogIndex: 1,
	}

	// Изменяем TxHash и LogIndex - только эти поля влияют на ключ
	variations := []Swap{
		func() Swap { s := baseSwap; s.TxHash = "0xdef"; return s }(),
		func() Swap { s := baseSwap; s.LogIndex = 2; return s }(),
	}

	baseKey := baseSwap.Key()
	for i, variation := range variations {
		varKey := variation.Key()
		if baseKey == varKey {
			t.Errorf("Variation %d should have different key from base: %s == %s", i, baseKey, varKey)
		}
	}
}
