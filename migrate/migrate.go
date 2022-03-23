package migrate

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"path"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	ds "github.com/ipfs/go-datastore"
	shell "github.com/ipfs/go-ipfs-api"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
)

const (
	PagesBeforeSleep = 5 // Up to 5000 entries given a default page size of 1000
	PaginationSleep  = 1 * time.Second

	PageRetryDelay        = 250 * time.Millisecond
	PageRequestTimeout    = 3 * time.Second
	NumPageRequestRetries = 3

	PinRetryDelay        = 250 * time.Millisecond
	PinRequestDelay      = 10 * time.Millisecond
	PinRequestTimeout    = 3 * time.Second
	NumPinRequestRetries = 3

	PinSuccessFilename = "pinSuccess.txt"
	PinFailureFilename = "pinFailure.txt"
)

var (
	// Use a concurrent map to keep track of the pin status of CIDs read from S3
	pinMap = sync.Map{}

	// Default context
	ctx = context.Background()

	// Wait group for goroutines
	wg = sync.WaitGroup{}

	// Stats
	foundCount      = 0
	convertedCount  = 0
	pinSuccessCount = 0
	pinFailureCount = 0
)

func Migrate(bucket, prefix, ipfsUrl, logPath string) {
	// Setup S3 access and pagination
	cfg := configureAWS()
	s3Client := s3.NewFromConfig(cfg)
	paginator := s3.NewListObjectsV2Paginator(s3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})

	// Create the log file path, and ignore if it already exists.
	if err := os.MkdirAll(logPath, 0770); err != nil {
		log.Fatal(err)
	}

	// Use a single shell to IPFS since it uses a thread-safe HTTP client
	ipfsShell := shell.NewShell(ipfsUrl)

	// If logs were written previously, load them, and pin any previous pin failure CIDs.
	_, pinFailureCids := readLogFiles(logPath)
	pinCids(ipfsShell, pinFailureCids)

	pageNum := 1
	for paginator.HasMorePages() {
		// Pagination is stateful in order to keep track of position and so should not be placed in a goroutine
		page, err := nextPage(paginator)
		if err != nil { // We've retried and still failed, exit the loop.
			log.Printf("retrieve page failed: %s", err)
			break
		}
		log.Printf("retrieved page: %d", pageNum)
		cids := cidsFromPage(page)
		if len(cids) > 0 {
			pinCids(ipfsShell, cids)
		} else {
			log.Printf("no CIDs found on page %d", pageNum)
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
	log.Printf("Done. Migration results: found %d, converted %d, pin success %d, pin failure %d", foundCount, convertedCount, pinSuccessCount, pinFailureCount)
}

func configureAWS() aws.Config {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatal(err)
	}
	return cfg
}

func blockToCid(key string) (string, error) {
	// All files will be either in `dag-jose` (multicodec 0x85) or `dag-cbor` (multicodec 0x71) format. Since `dag-cbor`
	// is a superset of `dag-jose`, use `dag-cbor` for the migration.
	cid, err := dshelp.DsKeyToCidV1(ds.NewKey(key), 0x71)
	return cid.String(), err
}

func nextPage(paginator *s3.ListObjectsV2Paginator) (*s3.ListObjectsV2Output, error) {
	nextFn := func() (*s3.ListObjectsV2Output, error) {
		// Use a new child context with timeout for each page retrieval attempt
		pageCtx, pageCancel := context.WithTimeout(ctx, PageRequestTimeout)
		defer pageCancel()

		page, err := paginator.NextPage(pageCtx)
		select {
		case <-pageCtx.Done():
			log.Printf("page failed: %s", pageCtx.Err())
			return nil, pageCtx.Err()
		default:
			// Fall through and return result
		}
		return page, err
	}
	for i := 0; i < NumPageRequestRetries; i++ {
		// Ref: https://stackoverflow.com/questions/45617758/defer-in-the-loop-what-will-be-better
		if page, err := nextFn(); err == nil {
			return page, nil
		}
		time.Sleep(time.Duration(math.Pow(2, float64(i))) * PageRetryDelay) // exponential backoff
	}
	return nil, errors.New("maximum retries exceeded")
}

func cidsFromPage(page *s3.ListObjectsV2Output) []string {
	cids := make([]string, 0, page.KeyCount)
	prevFoundCount := foundCount
	prevConvertedCount := convertedCount
	for _, object := range page.Contents {
		if object.Size != 0 { // filter out directories
			foundCount++
			// Convert file name to CID
			key := aws.ToString(object.Key)
			_, name := path.Split(key)
			cid, err := blockToCid(name)
			if err == nil {
				convertedCount++
				log.Printf("converted: key=%s, cid=%s", key, cid)
				cids = append(cids, cid)
			} else {
				log.Printf("convert failed: key=%s, err:%s", key, err)
			}
		}
	}
	log.Printf("keys found: %d", foundCount-prevFoundCount)
	log.Printf("keys converted: %d", convertedCount-prevConvertedCount)
	return cids
}

func pinCids(ipfsShell *shell.Shell, cids []string) {
	for _, cid := range cids {
		// Ref: https://github.com/golang/go/wiki/CommonMistakes#using-goroutines-on-loop-iterator-variables
		cidToPin := cid
		// Pin the CID if it wasn't in the status map, or it was but marked pin failure.
		if pinSuccess, found := pinMap.Load(cidToPin); !found || !pinSuccess.(bool) {
			// Pin requests are standalone and can be in a goroutine for parallelism. Use a wait group to synchronize
			// completion of all goroutines.
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := pinCid(ipfsShell, cidToPin)
				if err == nil {
					log.Printf("pin success: cid=%s", cidToPin)
					pinMap.Store(cidToPin, true)
				} else {
					log.Printf("pin failure: cid=%s, err=%s", cidToPin, err)
					pinMap.Store(cidToPin, false)
				}
			}()
			time.Sleep(PinRequestDelay)
		}
	}
	log.Printf("cids pinned: %d", len(cids))
}

func pinCid(ipfsShell *shell.Shell, cid string) error {
	pinFn := func() error {
		// Use a new child context with timeout for each pin request attempt
		pinCtx, pinCancel := context.WithTimeout(ctx, PinRequestTimeout)
		defer pinCancel()

		err := ipfsShell.Request("pin/add", cid).Option("recursive", false).Exec(pinCtx, nil)
		select {
		case <-pinCtx.Done():
			log.Printf("pin failed: cid=%s, err=%s", cid, pinCtx.Err())
			return pinCtx.Err()
		default:
			// Fall through
		}
		return err
	}
	for i := 0; i < NumPinRequestRetries; i++ {
		if err := pinFn(); err == nil {
			return nil
		}
		time.Sleep(time.Duration(math.Pow(2, float64(i))) * PinRetryDelay) // exponential backoff
	}
	return errors.New("maximum retries exceeded")
}

func readLogFiles(path string) ([]string, []string) {
	pinSuccessCids := readLogFile(path+"/"+PinSuccessFilename, true)
	pinFailureCids := readLogFile(path+"/"+PinFailureFilename, false)
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
			pinSuccessCount++
		} else {
			_, _ = fmt.Fprintln(pinFailure, key.(string))
			pinFailureCount++
		}
		return true
	})
}
