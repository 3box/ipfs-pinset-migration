package migrate

import (
	"math"
	"time"

	ds "github.com/ipfs/go-datastore"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
)

func acquireReqToken() {
	// Write to the buffered pin channel before making a new pin request. The capacity of the channel will
	// ensure that there are only a certain number of outstanding requests at any time. Trying to write to the
	// channel will block if it is full, and only resume after an outstanding request completes and a value is
	// read off the channel.
	pinCh <- 0
}

func releaseReqToken() {
	// Read from the buffered pin channel after pinning a batch of CIDs is complete. This will make room for
	// subsequent pin requests.
	<-pinCh
}

func exponentialBackoff(iteration int, max int, delay time.Duration) { // Zero-based iteration index
	if iteration < max-1 {
		time.Sleep(time.Duration(math.Pow(2, float64(iteration))) * delay) // exponential backoff
	}
}

func sliceBatcher(slice []string, batchSize int) [][]string {
	cidBatches := make([][]string, 0, (len(slice)+batchSize-1)/batchSize)
	for batchSize < len(slice) {
		slice, cidBatches = slice[batchSize:], append(cidBatches, slice[0:batchSize:batchSize])
	}
	return append(cidBatches, slice)
}

func blockToCid(key string) (string, error) {
	// All files will be either in `dag-jose` (multicodec 0x85) or `dag-cbor` (multicodec 0x71) format. Since `dag-cbor`
	// is a superset of `dag-jose`, use `dag-cbor` for the migration.
	cid, err := dshelp.DsKeyToCidV1(ds.NewKey(key), 0x71)
	return cid.String(), err
}

func isCidPinned(cid string) bool {
	pinSuccess, found := pinMap.Load(cid)
	return found && pinSuccess.(bool)
}
