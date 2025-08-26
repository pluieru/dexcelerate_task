package common

import "errors"

var (
	// ErrSnapshotNotFound возникает когда снапшот не найден
	ErrSnapshotNotFound = errors.New("snapshot not found")
)
