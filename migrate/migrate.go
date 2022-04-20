package migrate

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	shell "github.com/ipfs/go-ipfs-api"
)

const (
	PinRetryDelay      = 250 * time.Millisecond
	PinTimeout         = 10 * time.Second
	NumPinRetries      = 3
	PinBatchSize       = 50
	PinOutstandingReqs = 4 // For backpressure

	PinSuccessFilename = "pinSuccess.txt"
	PinFailureFilename = "pinFailure.txt"
)

var (
	// Default context
	ctx = context.Background()

	// Wait group for goroutines
	wg = sync.WaitGroup{}

	// Use a concurrent map to keep track of the pin status of CIDs read from S3
	pinMap = sync.Map{}

	// Use a buffered channel to apply backpressure and limit the number of outstanding pin requests to IPFS
	pinCh = make(chan int, PinOutstandingReqs)

	// Stats
	keysFoundCount     = uint32(0)
	keysConvertedCount = uint32(0)
	pinnedCount        = uint32(0)
	pinsFailedCount    = uint32(0)
	pinsRemainingCount = uint32(0)
)

type Migration interface {
	Migrate(logPath string, ipfsShell *shell.Shell) (int, int)
}

func Migrate(migration Migration, ipfsUrl, logPath string) {
	start := time.Now()

	// Create the log file path, and ignore if it already exists.
	if err := os.MkdirAll(logPath, 0770); err != nil {
		log.Fatal(err)
	}

	// Use a single shell to IPFS since it uses a thread-safe HTTP client
	ipfsShell := shell.NewShell(ipfsUrl)

	// If logs were written previously, load them, and pin any previous pin failure CIDs.
	pinFailedCids(logPath, ipfsShell)

	// Paginate through the pinstore and attempt to re-pin them
	pinSuccessCount, pinFailureCount := migration.Migrate(logPath, ipfsShell)

	log.Printf(
		"Done. Migration results: found %d, converted %d, pin success %d, pin failure %d, elapsed=%s",
		keysFoundCount,
		keysConvertedCount,
		pinSuccessCount,
		pinFailureCount,
		time.Since(start),
	)
}

func pinFailedCids(logPath string, ipfsShell *shell.Shell) (int, int) {
	_, pinFailureCids := readLogFiles(logPath)
	pinCids(ipfsShell, pinFailureCids)

	// Wait for all goroutines to complete, then write final logs.
	wg.Wait()
	return writeLogFiles(logPath)
}

func pinCids(ipfsShell *shell.Shell, cids []string) {
	if len(cids) == 0 {
		return
	}

	// Split the slice into batches and use a goroutine to pin all the CIDs in a batch
	batches := sliceBatcher(cids, PinBatchSize)
	for _, batch := range batches {
		// Ref: https://github.com/golang/go/wiki/CommonMistakes#using-goroutines-on-loop-iterator-variables
		batchToPin := batch
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Apply backpressure
			acquireReqToken()
			defer releaseReqToken()

			start := time.Now()
			err := pinCidBatch(ipfsShell, batchToPin)
			elapsed := time.Since(start)

			// Decrement the number of pins remaining regardless of whether pinning succeeded or failed
			count := uint32(len(batchToPin))
			pinsRemaining := atomic.AddUint32(&pinsRemainingCount, -count)
			if err == nil {
				log.Printf(
					"pinned batch in %s, total pinned=%d, total failed=%d, total remaining=%d",
					elapsed,
					atomic.AddUint32(&pinnedCount, count),
					atomic.LoadUint32(&pinsFailedCount),
					pinsRemaining,
				)
			} else {
				log.Printf(
					"pin failed in %s, total pinned=%d, total failed=%d, total remaining=%d, err=%s",
					elapsed,
					atomic.LoadUint32(&pinnedCount),
					atomic.AddUint32(&pinsFailedCount, count),
					pinsRemaining,
					err,
				)
			}
		}()
	}
}

func pinCidBatch(ipfsShell *shell.Shell, cids []string) error {
	storeFn := func(err error) error {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, cid := range cids {
				pinMap.Store(cid, err == nil)
			}
		}()
		return err
	}
	pinFn := func() error {
		// Use a new child context with timeout for each pin request attempt
		pinCtx, pinCancel := context.WithTimeout(ctx, PinTimeout)
		defer pinCancel()

		ch := make(chan error, 1)

		wg.Add(1)
		go func() {
			defer wg.Done()
			ch <- ipfsShell.Request("pin/add", cids...).Option("recursive", false).Exec(pinCtx, nil)
		}()
		for {
			select {
			case err := <-ch:
				return storeFn(err)
			case <-pinCtx.Done():
				return storeFn(pinCtx.Err())
			}
		}
	}
	for i := 0; i < NumPinRetries; i++ {
		if err := pinFn(); err == nil {
			return nil
		} else {
			log.Printf("pin failed: attempt=%d, err=%s", i+1, err)
		}
		exponentialBackoff(i, NumPinRetries, PinRetryDelay)
	}
	return errors.New("maximum retries exceeded")
}

func readLogFiles(path string) ([]string, []string) {
	pinSuccessCids := readLogFile(path+"/"+PinSuccessFilename, true)
	pinFailureCids := readLogFile(path+"/"+PinFailureFilename, false)

	// Set the number of pins remaining to the number of CIDs that failed to be pinned previously
	atomic.StoreUint32(&pinsRemainingCount, uint32(len(pinFailureCids)))

	log.Printf("read: pinSuccess=%d, pinFailure=%d", len(pinSuccessCids), len(pinFailureCids))
	return pinSuccessCids, pinFailureCids
}

func readLogFile(path string, status bool) []string {
	cids := make([]string, 0)
	f, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE, 0755)
	defer func(p *os.File) { _ = p.Close() }(f)
	if err == nil {
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := scanner.Text()
			pinMap.Store(line, status)
			cids = append(cids, line)
		}
	}
	return cids
}

func writeLogFiles(path string) (int, int) {
	pinSuccessCount := 0
	pinFailureCount := 0

	pinSuccessPath := path + "/" + PinSuccessFilename
	pinSuccess, err := os.OpenFile(pinSuccessPath, os.O_WRONLY|os.O_TRUNC, 0755)
	defer func(p *os.File) { _ = p.Close() }(pinSuccess)
	if err != nil {
		log.Printf("file create failed: path=%s, err=%s", pinSuccessPath, err)
		return 0, 0
	}
	pinFailurePath := path + "/" + PinFailureFilename
	pinFailure, err := os.OpenFile(pinFailurePath, os.O_WRONLY|os.O_TRUNC, 0755)
	defer func(u *os.File) { _ = u.Close() }(pinFailure)
	if err != nil {
		log.Printf("file create failed: path=%s, err=%s", pinFailurePath, err)
		return 0, 0
	}

	pinMap.Range(func(key, value interface{}) bool {
		// Ignore errors writing individual CIDs to file
		if value.(bool) {
			pinSuccessCount++
			_, _ = fmt.Fprintln(pinSuccess, key.(string))
		} else {
			pinFailureCount++
			_, _ = fmt.Fprintln(pinFailure, key.(string))
		}
		return true
	})

	log.Printf("wrote: pinSuccess=%d, pinFailure=%d", pinSuccessCount, pinFailureCount)
	return pinSuccessCount, pinFailureCount
}
