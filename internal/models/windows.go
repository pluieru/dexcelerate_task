package models

import (
	"strings"
	"time"
)

// Window представляет временное окно для агрегации
type Window string

const (
	Window5m  Window = "5m"
	Window1h  Window = "1h"
	Window24h Window = "24h"
)

// DefaultWindows содержит стандартные окна агрегации
var DefaultWindows = []Window{Window5m, Window1h, Window24h}

// ParseWindows парсит строку с окнами из переменной окружения
func ParseWindows(env string) []Window {
	if env == "" {
		return DefaultWindows
	}

	parts := strings.Split(env, ",")
	windows := make([]Window, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			windows = append(windows, Window(part))
		}
	}

	if len(windows) == 0 {
		return DefaultWindows
	}

	return windows
}

// Duration возвращает продолжительность окна
func (w Window) Duration() (time.Duration, error) {
	switch w {
	case Window5m:
		return 5 * time.Minute, nil
	case Window1h:
		return time.Hour, nil
	case Window24h:
		return 24 * time.Hour, nil
	default:
		return time.ParseDuration(string(w))
	}
}

// Validate проверяет что окно корректно
func (w Window) Validate() error {
	_, err := w.Duration()
	return err
}

// AlignToBucket выравнивает временную метку к началу бакета
func AlignToBucket(ts time.Time, step time.Duration) time.Time {
	nanos := ts.UnixNano()
	stepNanos := step.Nanoseconds()
	aligned := (nanos / stepNanos) * stepNanos
	return time.Unix(0, aligned).UTC()
}
