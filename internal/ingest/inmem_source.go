package ingest

import (
	"context"
	"dexc_conf/internal/models"
	"fmt"
	"log/slog"
	"math/rand"
	"time"
)

// InMemorySource generates demo swap events in memory
type InMemorySource struct {
	config SourceConfig
	tokens []string
	rng    *rand.Rand
	buffer []models.Swap
}

// NewInMemorySource creates a new in-memory event source
func NewInMemorySource(config SourceConfig) *InMemorySource {

	tokens := generateDemoTokens(config.TokenCount)

	return &InMemorySource{
		config: config,
		tokens: tokens,
		rng:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Run starts event generation
func (s *InMemorySource) Run(ctx context.Context, out chan<- models.Swap) error {
	slog.Info("Starting in-memory event source",

		slog.Int("events_per_second", s.config.EventsPerSecond),
		slog.Int("token_count", len(s.tokens)))

	interval := time.Second / time.Duration(s.config.EventsPerSecond)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	if s.config.BackpressureStrategy == BackpressureBuffer {
		go s.bufferDrainRoutine(ctx, out)
	}

	eventID := 0

	for {
		select {
		case <-ctx.Done():
			slog.Info("Stopping in-memory event source")
			return ctx.Err()

		case <-ticker.C:
			swap := s.generateSwap(eventID)
			eventID++

			if err := s.sendEvent(ctx, out, swap); err != nil {
				if err == ctx.Err() {
					return err
				}
				slog.Warn("Failed to send event", slog.String("error", err.Error()))
			}
		}
	}
}

// sendEvent sends an event using the configured backpressure strategy
func (s *InMemorySource) sendEvent(ctx context.Context, out chan<- models.Swap, swap models.Swap) error {
	switch s.config.BackpressureStrategy {
	case BackpressureBlock:
		select {
		case out <- swap:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}

	case BackpressureBuffer:
		select {
		case out <- swap:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		default:
			return s.bufferEvent(swap)
		}

	case BackpressureDrop:
		fallthrough
	default:
		select {
		case out <- swap:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		default:
			slog.Warn("Output channel full, dropping event", slog.String("event_id", swap.ID))
			return nil
		}
	}
}

// bufferEvent adds an event to the internal buffer
func (s *InMemorySource) bufferEvent(swap models.Swap) error {
	if len(s.buffer) >= s.config.BackpressureBufferSize {
		s.buffer = s.buffer[1:]
		slog.Warn("Internal buffer full, dropping oldest event")
	}

	s.buffer = append(s.buffer, swap)
	slog.Debug("Event buffered",
		slog.String("event_id", swap.ID),
		slog.Int("buffer_size", len(s.buffer)))
	return nil
}

// bufferDrainRoutine periodically tries to send events from buffer
func (s *InMemorySource) bufferDrainRoutine(ctx context.Context, out chan<- models.Swap) {
	drainTicker := time.NewTicker(10 * time.Millisecond)
	defer drainTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-drainTicker.C:
			if len(s.buffer) > 0 {
				select {
				case out <- s.buffer[0]:
					s.buffer = s.buffer[1:]
				case <-ctx.Done():
					return
				default:
				}
			}
		}
	}
}

// generateSwap generates a random swap event
func (s *InMemorySource) generateSwap(eventID int) models.Swap {
	now := time.Now().UTC()

	jitter := time.Duration(s.rng.Intn(2000)) * time.Millisecond
	eventTime := now.Add(-jitter)

	maxSkew := 30 * time.Second
	if eventTime.After(now.Add(maxSkew)) {
		eventTime = now.Add(maxSkew)
	}

	token := s.tokens[s.rng.Intn(len(s.tokens))]

	amount := s.generateAmount()
	usd := s.generateUSD(amount)
	side := s.generateSide()
	who := s.generateAddress()
	txHash := s.generateTxHash()

	return models.Swap{
		ID:       fmt.Sprintf("demo_%d", eventID),
		Who:      who,
		Token:    token,
		Amount:   amount,
		USD:      usd,
		Side:     side,
		TS:       eventTime,
		TxHash:   txHash,
		LogIndex: s.rng.Intn(10), // случайный индекс лога
	}
}

// generateDemoTokens creates a list of demo tokens
func generateDemoTokens(count int) []string {
	baseTokens := []string{
		"WETH", "USDC", "USDT", "DAI", "WBTC",
		"UNI", "LINK", "AAVE", "COMP", "MKR",
		"SUSHI", "CRV", "YFI", "SNX", "1INCH",
		"MATIC", "FTM", "AVAX", "BNB", "ADA",
	}

	if count <= len(baseTokens) {
		return baseTokens[:count]
	}

	tokens := make([]string, count)
	copy(tokens, baseTokens)

	for i := len(baseTokens); i < count; i++ {
		tokens[i] = fmt.Sprintf("TOKEN%d", i-len(baseTokens)+1)
	}

	return tokens
}

func (s *InMemorySource) generateAmount() float64 {
	base := s.rng.ExpFloat64() * 100
	if base < 0.001 {
		base = 0.001
	}
	if base > 10000 {
		base = 10000
	}
	return base
}

func (s *InMemorySource) generateUSD(amount float64) float64 {
	priceRange := []float64{0.1, 1, 10, 100, 1000, 5000}
	price := priceRange[s.rng.Intn(len(priceRange))]

	variance := 0.9 + s.rng.Float64()*0.2 // ±10%
	price *= variance

	return amount * price
}

func (s *InMemorySource) generateSide() models.Side {
	if s.rng.Float32() < 0.5 {
		return models.SideBuy
	}
	return models.SideSell
}

func (s *InMemorySource) generateAddress() string {
	chars := "0123456789abcdef"
	address := "0x"
	for i := 0; i < 40; i++ {
		address += string(chars[s.rng.Intn(len(chars))])
	}
	return address
}

func (s *InMemorySource) generateTxHash() string {
	chars := "0123456789abcdef"
	hash := "0x"
	for i := 0; i < 64; i++ {
		hash += string(chars[s.rng.Intn(len(chars))])
	}
	return hash
}

func (s *InMemorySource) GetStats() SourceStats {
	return SourceStats{
		Type:            "in-memory",
		EventsPerSecond: s.config.EventsPerSecond,
		TokenCount:      len(s.tokens),
		Tokens:          s.tokens,
	}
}

type SourceStats struct {
	Type            string   `json:"type"`              // type
	EventsPerSecond int      `json:"events_per_second"` // events per second
	TokenCount      int      `json:"token_count"`       // token count
	Tokens          []string `json:"tokens"`            // tokens
}
