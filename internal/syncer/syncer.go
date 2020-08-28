package syncer

import (
	"github.com/shirou/gopsutil"
	"godrive/internal/gdrive"
)

// This is package where the main logic of the background sync process lies

var (
	checkInterval int = 10
	drivestore    *gdrive.GDStore
)

// SyncST internal data of syncer
type SyncST struct {
}

// NewSyncer new syncer instance
func NewSyncer() *SyncST {
	ns := new(SyncST)

	return ns
}

// Start watching for changes and sync
func (sy *SyncST) Start() error {
	return nil
}

func (sy *SyncST) initialCheck() error {

	return nil
}

func (sy *SyncST) Download() error {
	return nil
}

func (sy *SyncST) Upload() error {
	return nil
}
