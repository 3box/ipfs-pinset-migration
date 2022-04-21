package migrate

import (
	"log"
	"sync/atomic"

	"github.com/ipfs/go-ds-leveldb"
	shell "github.com/ipfs/go-ipfs-api"
)

var _ Migration = &MigrateFs{}

const PageSize = 1000

type MigrateFs struct {
	Path string
}

func (*MigrateFs) printConfig() {
	log.Print("Configuration:")
	log.Printf("\tPinRetryDelay=%s", PinRetryDelay)
	log.Printf("\tPinTimeout=%s", PinTimeout)
	log.Printf("\tNumPinRetries=%d", NumPinRetries)
	log.Printf("\tPinBatchSize=%d", PinBatchSize)
	log.Printf("\tPinOutstandingReqs=%d", PinOutstandingReqs)
}

func (m *MigrateFs) Migrate(logPath string, ipfsShell *shell.Shell) (int, int) {
	// Log the configuration so that we know what settings are being used for the current run
	m.printConfig()

	d, err := leveldb.NewDatastore(m.Path, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()

	keysFound := uint32(0)
	keysConverted := uint32(0)
	pinsRemaining := uint32(0)
	printCounts := func() {
		log.Printf("keys found=%d, total found=%d", keysFound, atomic.AddUint32(&keysFoundCount, keysFound))
		log.Printf("keys converted=%d, total converted=%d", keysConverted, atomic.AddUint32(&keysConvertedCount, keysConverted))
		log.Printf("cids not pinned=%d, total remaining=%d", pinsRemaining, atomic.AddUint32(&pinsRemainingCount, pinsRemaining))
	}

	cids := make([]string, 0, 1)
	iter := d.DB.NewIterator(nil, nil)
	for iter.Next() {
		keysFound++
		key := string(iter.Key())
		// Convert file name to CID
		cid, err := blockToCid(key)
		if err == nil {
			keysConverted++
			if !isCidPinned(cid) {
				pinsRemaining++
				cids = append(cids, cid)
			}
		} else {
			log.Printf("convert failed: key=%s, err:%s", key, err)
		}
		// Print counts and pin CIDs at the end of each page
		if keysFound%PageSize == 0 {
			printCounts()
			pinCids(ipfsShell, cids)
			// Reset the CID array for the next page
			cids = make([]string, 0, 1)
		}
	}
	// If we end iteration in the middle of a page, reprint the counts and pin accumulated CIDs because we know they'll
	// be different from the counts and CIDs at the end of the previous page.
	if keysFound%PageSize != 0 {
		printCounts()
		pinCids(ipfsShell, cids)
	}

	iter.Release()
	wg.Wait()
	return writeLogFiles(logPath)
}
