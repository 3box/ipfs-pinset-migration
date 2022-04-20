package migrate

import (
	"context"
	"errors"
	"log"
	"path"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	shell "github.com/ipfs/go-ipfs-api"
)

var _ Migration = &MigrateS3{}

const (
	PagesBeforeSleep     = 5 // Up to 5000 entries given a default page size of 1000
	PaginationSleep      = 1 * time.Second
	PaginationRetryDelay = 250 * time.Millisecond
	PaginationTimeout    = 3 * time.Second
	NumPaginationRetries = 3
)

type MigrateS3 struct {
	Bucket string
	Prefix string
}

func (*MigrateS3) printConfig() {
	log.Print("Configuration:")
	log.Printf("\tPagesBeforeSleep=%d", PagesBeforeSleep)
	log.Printf("\tPaginationSleep=%s", PaginationSleep)
	log.Printf("\tPaginationRetryDelay=%s", PaginationRetryDelay)
	log.Printf("\tPaginationTimeout=%s", PaginationTimeout)
	log.Printf("\tNumPaginationRetries=%d", NumPaginationRetries)
	log.Printf("\tPinRetryDelay=%s", PinRetryDelay)
	log.Printf("\tPinTimeout=%s", PinTimeout)
	log.Printf("\tNumPinRetries=%d", NumPinRetries)
	log.Printf("\tPinBatchSize=%d", PinBatchSize)
	log.Printf("\tPinOutstandingReqs=%d", PinOutstandingReqs)
}

func (m *MigrateS3) Migrate(logPath string, ipfsShell *shell.Shell) (int, int) {
	// Log the configuration so that we know what settings are being used for the current run
	m.printConfig()

	paginator := s3Paginator(m.Bucket, m.Prefix)
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
			log.Printf("no cids to be pinned found on page %d", pageNum)
		}
		if (pageNum % PagesBeforeSleep) == 0 {
			// Sleep before pulling more pages to avoid S3 throttling
			time.Sleep(PaginationSleep)
		}
		pageNum++
	}

	// Wait for all goroutines to complete, then write final logs.
	wg.Wait()
	return writeLogFiles(logPath)
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

func s3Paginator(bucket, prefix string) *s3.ListObjectsV2Paginator {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatal(err)
	}
	s3Client := s3.NewFromConfig(cfg)
	return s3.NewListObjectsV2Paginator(s3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
}

func cidsFromS3Page(page *s3.ListObjectsV2Output) []string {
	cids := make([]string, 0, page.KeyCount)
	pageKeysFound := uint32(0)
	pageKeysConverted := uint32(0)
	pagePinsRemaining := uint32(0)
	for _, object := range page.Contents {
		if object.Size != 0 { // filter out directories
			pageKeysFound++
			// Convert file name to CID
			key := aws.ToString(object.Key)
			_, name := path.Split(key)
			cid, err := blockToCid(name)
			if err == nil {
				pageKeysConverted++
				if !isCidPinned(cid) {
					pagePinsRemaining++
					cids = append(cids, cid)
				}
			} else {
				log.Printf("convert failed: key=%s, err:%s", key, err)
			}
		}
	}

	log.Printf("keys found=%d, total found=%d", pageKeysFound, atomic.AddUint32(&keysFoundCount, pageKeysFound))
	log.Printf("keys converted=%d, total converted=%d", pageKeysConverted, atomic.AddUint32(&keysConvertedCount, pageKeysConverted))
	log.Printf("cids not pinned=%d, total remaining=%d", pagePinsRemaining, atomic.AddUint32(&pinsRemainingCount, pagePinsRemaining))
	return cids
}
