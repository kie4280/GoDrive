package syncer

// This is package where the main logic of the background sync process lies

var (
	checkInterval int = 10
)

// SyncST internal data of syncer
type SyncST struct {
}

// NewSyncer new syncer instance
func NewSyncer() *SyncST {

}

// Start watching for changes and sync
func (sy *SyncST) Start() error {

}
