package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"nhooyr.io/websocket"
)

// Hub управляет WebSocket соединениями и рассылкой сообщений
type Hub struct {
	mu sync.RWMutex

	// Конфигурация
	writeBuffer int

	// Соединения
	clients     map[*Client]bool            // активные клиенты
	subscribers map[string]map[*Client]bool // подписки по токенам: token -> clients
	allSubs     map[*Client]bool            // клиенты, подписанные на все токены

	// Каналы
	register   chan *Client       // регистрация клиентов
	unregister chan *Client       // отключение клиентов
	broadcast  chan UpdateMessage // рассылка сообщений

	// Контроль
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// Client представляет WebSocket клиента
type Client struct {
	hub    *Hub
	conn   *websocket.Conn
	send   chan UpdateMessage
	filter SubscriptionFilter
	id     string

	// Контроль
	ctx    context.Context
	cancel context.CancelFunc
}

// NewHub создает новый WebSocket hub
func NewHub(writeBuffer int) *Hub {
	return &Hub{
		writeBuffer: writeBuffer,
		clients:     make(map[*Client]bool),
		subscribers: make(map[string]map[*Client]bool),
		allSubs:     make(map[*Client]bool),
		register:    make(chan *Client),
		unregister:  make(chan *Client),
		broadcast:   make(chan UpdateMessage, 256), // буферизованный канал

		stopCh: make(chan struct{}),
	}
}

// Start запускает hub
func (h *Hub) Start() {
	h.wg.Add(1)
	go h.run()
}

// Stop останавливает hub
func (h *Hub) Stop() {
	close(h.stopCh)
	h.wg.Wait()
}

// run основной цикл hub
func (h *Hub) run() {
	defer h.wg.Done()

	ticker := time.NewTicker(30 * time.Second) // ping каждые 30 секунд
	defer ticker.Stop()

	for {
		select {
		case client := <-h.register:
			h.registerClient(client)

		case client := <-h.unregister:
			h.unregisterClient(client)

		case message := <-h.broadcast:
			h.broadcastMessage(message)

		case <-ticker.C:
			h.pingClients()

		case <-h.stopCh:
			h.closeAllClients()
			return
		}
	}
}

// registerClient регистрирует нового клиента
func (h *Hub) registerClient(client *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.clients[client] = true

	// Подписываем на токены
	if len(client.filter.Tokens) == 0 {
		// Подписка на все токены
		h.allSubs[client] = true
	} else {
		// Подписка на конкретные токены
		for _, token := range client.filter.Tokens {
			if h.subscribers[token] == nil {
				h.subscribers[token] = make(map[*Client]bool)
			}
			h.subscribers[token][client] = true
		}
	}

	slog.Info("WebSocket client connected",

		slog.String("client_id", client.id),
		slog.Int("total_clients", len(h.clients)),
		slog.Any("tokens", client.filter.Tokens))
}

// unregisterClient отключает клиента
func (h *Hub) unregisterClient(client *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if _, ok := h.clients[client]; ok {
		delete(h.clients, client)
		close(client.send)

		// Удаляем из подписок
		delete(h.allSubs, client)
		for token, clients := range h.subscribers {
			delete(clients, client)
			if len(clients) == 0 {
				delete(h.subscribers, token)
			}
		}

		slog.Info("WebSocket client disconnected",

			slog.String("client_id", client.id),
			slog.Int("total_clients", len(h.clients)))
	}
}

// broadcastMessage рассылает сообщение подписчикам
func (h *Hub) broadcastMessage(message UpdateMessage) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	var targetClients []*Client

	// Собираем целевых клиентов
	if message.Token != "" {
		// Клиенты, подписанные на конкретный токен
		if clients, exists := h.subscribers[message.Token]; exists {
			for client := range clients {
				targetClients = append(targetClients, client)
			}
		}
	}

	// Клиенты, подписанные на все токены
	for client := range h.allSubs {
		targetClients = append(targetClients, client)
	}

	// Отправляем сообщение
	for _, client := range targetClients {
		select {
		case client.send <- message:

		default:
			// Канал заполнен, закрываем соединение
			h.unregisterClient(client)
			client.cancel()
		}
	}

	slog.Debug("Broadcasted WebSocket message",

		slog.String("type", message.Type),
		slog.String("token", message.Token),
		slog.Int("recipients", len(targetClients)))
}

// pingClients отправляет ping всем клиентам
func (h *Hub) pingClients() {
	h.mu.RLock()
	clients := make([]*Client, 0, len(h.clients))
	for client := range h.clients {
		clients = append(clients, client)
	}
	h.mu.RUnlock()

	for _, client := range clients {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := client.conn.Ping(ctx); err != nil {
			slog.Debug("Ping failed, disconnecting client",

				slog.String("client_id", client.id),
				slog.String("error", err.Error()))
			h.unregister <- client
			client.cancel()
		}
		cancel()
	}
}

// closeAllClients закрывает все соединения
func (h *Hub) closeAllClients() {
	h.mu.Lock()
	defer h.mu.Unlock()

	for client := range h.clients {
		client.cancel()
		close(client.send)
	}

	h.clients = make(map[*Client]bool)
	h.subscribers = make(map[string]map[*Client]bool)
	h.allSubs = make(map[*Client]bool)

}

// ServeWS обрабатывает WebSocket подключение
func (h *Hub) ServeWS(w http.ResponseWriter, r *http.Request) {
	// Принимаем WebSocket соединение
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: true,          // для разработки
		OriginPatterns:     []string{"*"}, // разрешаем любые origins
	})
	if err != nil {
		slog.Error("Failed to accept WebSocket connection",

			slog.String("error", err.Error()))
		return
	}

	// Парсим фильтр подписки из query параметров
	filter := h.parseSubscriptionFilter(r)

	// Создаем клиента
	ctx, cancel := context.WithCancel(r.Context())
	clientID := fmt.Sprintf("client_%d", time.Now().UnixNano())

	client := &Client{
		hub:    h,
		conn:   conn,
		send:   make(chan UpdateMessage, h.writeBuffer),
		filter: filter,
		id:     clientID,
		ctx:    ctx,
		cancel: cancel,
	}

	// Регистрируем клиента
	h.register <- client

	// Запускаем горутины для чтения и записи
	go client.writePump()
	go client.readPump()
}

// parseSubscriptionFilter парсит фильтр подписки из URL
func (h *Hub) parseSubscriptionFilter(r *http.Request) SubscriptionFilter {
	tokenParam := r.URL.Query().Get("token")
	if tokenParam == "" {
		return SubscriptionFilter{Tokens: []string{}} // подписка на все токены
	}

	// Разделяем токены по запятой
	tokens := strings.Split(tokenParam, ",")
	for i, token := range tokens {
		tokens[i] = strings.TrimSpace(token)
	}

	return SubscriptionFilter{Tokens: tokens}
}

// readPump читает сообщения от клиента
func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.CloseNow()
	}()

	// Устанавливаем лимиты
	c.conn.SetReadLimit(512) // максимум 512 байт на сообщение

	for {
		// Читаем сообщение (в основном для поддержания соединения)
		_, _, err := c.conn.Read(c.ctx)
		if err != nil {
			if websocket.CloseStatus(err) != websocket.StatusNormalClosure {
				slog.Debug("WebSocket read error",

					slog.String("client_id", c.id),
					slog.String("error", err.Error()))
			}
			break
		}
	}
}

// writePump отправляет сообщения клиенту
func (c *Client) writePump() {
	ticker := time.NewTicker(54 * time.Second) // ping каждые 54 секунды
	defer ticker.Stop()

	for {
		select {
		case message, ok := <-c.send:
			writeCtx, cancel := context.WithTimeout(c.ctx, 10*time.Second)

			if !ok {
				// Канал закрыт
				c.conn.Close(websocket.StatusNormalClosure, "")
				cancel()
				return
			}

			// Отправляем сообщение
			if err := c.writeMessage(writeCtx, message); err != nil {
				slog.Debug("WebSocket write error",

					slog.String("client_id", c.id),
					slog.String("error", err.Error()))
				cancel()
				return
			}
			cancel()

		case <-ticker.C:
			// Отправляем ping
			writeCtx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
			if err := c.conn.Ping(writeCtx); err != nil {
				slog.Debug("WebSocket ping error",

					slog.String("client_id", c.id),
					slog.String("error", err.Error()))
				cancel()
				return
			}
			cancel()

		case <-c.ctx.Done():
			return
		}
	}
}

// writeMessage отправляет сообщение в WebSocket
func (c *Client) writeMessage(ctx context.Context, message UpdateMessage) error {
	data, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	return c.conn.Write(ctx, websocket.MessageText, data)
}

// GetStats возвращает статистику hub
func (h *Hub) GetStats() HubStats {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return HubStats{
		TotalClients:    len(h.clients),
		AllSubsClients:  len(h.allSubs),
		TokenSubsCount:  len(h.subscribers),
		WriteBufferSize: h.writeBuffer,
	}
}

// HubStats содержит статистику WebSocket hub
type HubStats struct {
	TotalClients    int `json:"total_clients"`     // общее количество клиентов
	AllSubsClients  int `json:"all_subs_clients"`  // клиенты с подпиской на все токены
	TokenSubsCount  int `json:"token_subs_count"`  // количество токенов с подписчиками
	WriteBufferSize int `json:"write_buffer_size"` // размер буфера записи
}
