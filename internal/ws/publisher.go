package ws

import (
	"context"
	"dexc_conf/internal/models"
	"time"
)

// Publisher представляет интерфейс для публикации обновлений через WebSocket
type Publisher interface {
	// PublishAggregate публикует обновление агрегатов для токена
	PublishAggregate(ctx context.Context, token string, agg models.Aggregates) error

	// PublishStats публикует статистику токена
	PublishStats(ctx context.Context, stats models.TokenStats) error

	// Close закрывает publisher
	Close() error
}

// UpdateMessage представляет сообщение с обновлением для WebSocket клиентов
type UpdateMessage struct {
	Type      string      `json:"type"`      // тип сообщения: "aggregate", "stats"
	Token     string      `json:"token"`     // токен
	Data      interface{} `json:"data"`      // данные (models.Aggregates или models.TokenStats)
	Timestamp int64       `json:"timestamp"` // Unix timestamp в миллисекундах
}

// SubscriptionFilter определяет фильтр для подписок
type SubscriptionFilter struct {
	Tokens []string `json:"tokens"` // список токенов для подписки, пустой = все токены
}

// HubPublisher реализует Publisher используя Hub
type HubPublisher struct {
	hub *Hub
}

// NewHubPublisher создает новый publisher на основе hub
func NewHubPublisher(hub *Hub) *HubPublisher {
	return &HubPublisher{hub: hub}
}

// PublishAggregate публикует обновление агрегатов для токена
func (p *HubPublisher) PublishAggregate(ctx context.Context, token string, agg models.Aggregates) error {
	message := UpdateMessage{
		Type:      "aggregate",
		Token:     token,
		Data:      agg,
		Timestamp: time.Now().UnixMilli(),
	}

	select {
	case p.hub.broadcast <- message:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		// Канал заполнен, пропускаем сообщение
		return nil
	}
}

// PublishStats публикует статистику токена
func (p *HubPublisher) PublishStats(ctx context.Context, stats models.TokenStats) error {
	message := UpdateMessage{
		Type:      "stats",
		Token:     stats.Token,
		Data:      stats,
		Timestamp: time.Now().UnixMilli(),
	}

	select {
	case p.hub.broadcast <- message:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		// Канал заполнен, пропускаем сообщение
		return nil
	}
}

// Close закрывает publisher
func (p *HubPublisher) Close() error {
	// Hub управляется отдельно, поэтому здесь ничего не делаем
	return nil
}
