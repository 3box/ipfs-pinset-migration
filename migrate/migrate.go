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

	"github.com/aws/aws-sdk-go-v2/service/s3"
	shell "github.com/ipfs/go-ipfs-api"
)

const (
	PagesBeforeSleep     = 5 // Up to 5000 entries given a default page size of 1000
	PaginationSleep      = 1 * time.Second
	PaginationRetryDelay = 250 * time.Millisecond
	PaginationTimeout    = 3 * time.Second
	NumPaginationRetries = 3

	PinRetryDelay      = 250 * time.Millisecond
	PinTimeout         = 10 * time.Second
	NumPinRetries      = 3
	PinBatchSize       = 40
	PinOutstandingReqs = 50 // For backpressure

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
	keyFoundCount     = uint32(0)
	keyConvertedCount = uint32(0)
	pinSuccessCount   = uint32(0)
	pinFailureCount   = uint32(0)
	pinRemainingCount = uint32(0)
)

func Migrate(bucket, prefix, ipfsUrl, logPath string) {
	start := time.Now()

	// Create the log file path, and ignore if it already exists.
	if err := os.MkdirAll(logPath, 0770); err != nil {
		log.Fatal(err)
	}

	// Use a single shell to IPFS since it uses a thread-safe HTTP client
	ipfsShell := shell.NewShell(ipfsUrl)

	// If logs were written previously, load them, and pin any previous pin failure CIDs.
	pinFailedCids(logPath, ipfsShell)
	migratePinstore(bucket, prefix, logPath, ipfsShell)

	// Make a final attempt to pin any CIDs we failed to pin above
	pinFailedCids(logPath, ipfsShell)

	log.Printf("Done. Migration results: found %d, converted %d, pin success %d, pin failure %d, elapsed=%s", atomic.LoadUint32(&keyFoundCount), atomic.LoadUint32(&keyConvertedCount), atomic.LoadUint32(&pinSuccessCount), atomic.LoadUint32(&pinFailureCount), time.Since(start))
}

func pinFailedCids(logPath string, ipfsShell *shell.Shell) {
	_, pinFailureCids := readLogFiles(logPath)
	pinCids(ipfsShell, pinFailureCids)

	// Wait for all goroutines to complete, then write final logs.
	wg.Wait()
	writeLogFiles(logPath)
}

func migratePinstore(bucket, prefix, logPath string, ipfsShell *shell.Shell) {
	paginator := s3Paginator(bucket, prefix)
	pageNum := 1
	for paginator.HasMorePages() {
		// Pagination is stateful in order to keep track of position and so should not be placed in a goroutine
		page, err := nextPage(paginator)
		if err != nil { // We've retried and still failed, exit the loop.
			log.Printf("pagination failed: %s", err)
			break
		}
		log.Printf("retrieved page: %d", pageNum)
		cids := cidsFromS3Page(page)
		if len(cids) > 0 {
			// Pin requests are standalone and can be in a goroutine for parallelism. Use a wait group to synchronize
			// completion of all goroutines.
			wg.Add(1)
			go func() {
				defer wg.Done()
				pinCids(ipfsShell, cids)
			}()
		} else {
			log.Printf("no unpinned cids found on page %d", pageNum)
		}
		if (pageNum % PagesBeforeSleep) == 0 {
			// Sleep before pulling more pages to avoid S3 throttling
			time.Sleep(PaginationSleep)
		}
		pageNum++
	}
	// Wait for all goroutines to complete, then write final logs.
	wg.Wait()
	writeLogFiles(logPath)
}

func nextPage(paginator *s3.ListObjectsV2Paginator) (*s3.ListObjectsV2Output, error) {
	nextFn := func() (*s3.ListObjectsV2Output, error) {
		// Use a new child context with timeout for each page retrieval attempt
		pageCtx, pageCancel := context.WithTimeout(ctx, PaginationTimeout)
		defer pageCancel()

		type pageResult struct {
			page *s3.ListObjectsV2Output
			err  error
		}
		// Buffered channel with a single slot
		ch := make(chan pageResult, 1)

		wg.Add(1)
		go func() {
			defer wg.Done()
			page, err := paginator.NextPage(pageCtx)
			ch <- pageResult{page: page, err: err}
		}()
		for {
			select {
			case val := <-ch:
				return val.page, val.err
			case <-pageCtx.Done():
				return nil, pageCtx.Err()
			}
		}
	}
	for i := 0; i < NumPaginationRetries; i++ {
		// Ref: https://stackoverflow.com/questions/45617758/defer-in-the-loop-what-will-be-better
		if page, err := nextFn(); err == nil {
			return page, nil
		} else {
			log.Printf("pagination failed: attempt=%d, err=%s", i+1, err)
		}
		exponentialBackoff(i, NumPaginationRetries, PaginationRetryDelay)
	}
	return nil, errors.New("maximum retries exceeded")
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
			count := uint32(len(batchToPin))
			// Decrement the number of pins remaining regardless of whether pinning succeeded or failed
			atomic.AddUint32(&pinRemainingCount, -count)
			if err == nil {
				atomic.AddUint32(&pinSuccessCount, count)
				log.Printf("pinned batch in %s, remaining cids=%d", elapsed, atomic.LoadUint32(&pinRemainingCount))
			} else {
				atomic.AddUint32(&pinFailureCount, count)
				log.Printf("pin failed in %s, remaining cids=%d, err=%s", elapsed, atomic.LoadUint32(&pinRemainingCount), err)
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
		}
		exponentialBackoff(i, NumPinRetries, PinRetryDelay)
	}
	return errors.New("maximum retries exceeded")
}

func readLogFiles(path string) ([]string, []string) {
	pinSuccessCids := readLogFile(path+"/"+PinSuccessFilename, true)
	pinFailureCids := readLogFile(path+"/"+PinFailureFilename, false)
	atomic.AddUint32(&pinSuccessCount, uint32(len(pinSuccessCids)))
	atomic.AddUint32(&pinFailureCount, uint32(len(pinFailureCids)))
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

func writeLogFiles(path string) {
	pinSuccessPath := path + "/" + PinSuccessFilename
	pinSuccess, err := os.OpenFile(pinSuccessPath, os.O_WRONLY|os.O_TRUNC, 0755)
	defer func(p *os.File) { _ = p.Close() }(pinSuccess)
	if err != nil {
		log.Printf("file create failed: path=%s, err=%s", pinSuccessPath, err)
		return
	}
	pinFailurePath := path + "/" + PinFailureFilename
	pinFailure, err := os.OpenFile(pinFailurePath, os.O_WRONLY|os.O_TRUNC, 0755)
	defer func(u *os.File) { _ = u.Close() }(pinFailure)
	if err != nil {
		log.Printf("file create failed: path=%s, err=%s", pinFailurePath, err)
		return
	}

	pinMap.Range(func(key, value interface{}) bool {
		// Ignore errors writing individual CIDs to file
		if value.(bool) {
			_, _ = fmt.Fprintln(pinSuccess, key.(string))
		} else {
			_, _ = fmt.Fprintln(pinFailure, key.(string))
		}
		return true
	})
}
