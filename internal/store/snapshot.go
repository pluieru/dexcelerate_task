package store

import (
	"context"
	"dexc_conf/internal/models"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type Snapshot struct {
	Version    string                                `json:"version"`     // версия формата снапшота
	Timestamp  time.Time                             `json:"timestamp"`   // время создания снапшота
	Windows    []models.Window                       `json:"windows"`     // настроенные окна
	BucketStep time.Duration                         `json:"bucket_step"` // размер шага бакета
	Watermark  time.Time                             `json:"watermark"`   // watermark обработки
	Data       map[string]map[models.Window][]Bucket `json:"data"`        // данные: token -> window -> buckets
}

type SnapshotManager struct {
	path string
}

func NewSnapshotManager(path string) *SnapshotManager {
	return &SnapshotManager{path: path}
}

func (sm *SnapshotManager) Save(ctx context.Context, snapshot Snapshot) error {
	snapshot.Version = "1.0"
	snapshot.Timestamp = time.Now().UTC()

	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal snapshot: %w", err)
	}

	tmpPath := sm.path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write snapshot to temp file: %w", err)
	}

	if err := os.Rename(tmpPath, sm.path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to rename snapshot file: %w", err)
	}

	return nil
}

func (sm *SnapshotManager) Load(ctx context.Context) (Snapshot, error) {
	var snapshot Snapshot

	if _, err := os.Stat(sm.path); os.IsNotExist(err) {
		return snapshot, nil
	}

	data, err := os.ReadFile(sm.path)
	if err != nil {
		return snapshot, fmt.Errorf("failed to read snapshot file: %w", err)
	}

	if err := json.Unmarshal(data, &snapshot); err != nil {
		return snapshot, fmt.Errorf("failed to unmarshal snapshot: %w", err)
	}

	return snapshot, nil
}

func (sm *SnapshotManager) Delete() error {
	if err := os.Remove(sm.path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete snapshot file: %w", err)
	}
	return nil
}

func (sm *SnapshotManager) Exists() bool {
	_, err := os.Stat(sm.path)
	return err == nil
}
