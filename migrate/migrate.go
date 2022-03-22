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

	RequestTimeout    = 3 * time.Second
	RetryDelay        = 250 * time.Millisecond
	NumRequestRetries = 3

	PinnedFilename   = "pinned.txt"
	UnpinnedFilename = "unpinned.txt"
)

var (
	// Use a concurrent map to keep track of the pin status of CIDs read from S3
	pinMap = sync.Map{}

	// Default context
	ctx = context.Background()

	// Stats
	foundCount     = 0
	convertedCount = 0
	pinnedCount    = 0
	unpinnedCount  = 0
)

func Migrate(bucket, prefix, ipfsUrl, statusPath string) {
	// Setup S3 access and pagination
	cfg := configureAWS()
	s3Client := s3.NewFromConfig(cfg)
	paginator := s3.NewListObjectsV2Paginator(s3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})

	// Create the status file path, and ignore if it already exists.
	if err := os.MkdirAll(statusPath, 0770); err != nil {
		log.Fatal(err)
	}

	// Use a single shell to IPFS since it uses a thread-safe HTTP client
	ipfsShell := shell.NewShell(ipfsUrl)

	// If statuses were already written previously, load them, and pin any unpinned CIDs.
	_, unpinnedCids := readPinStatuses(statusPath)
	pinCids(ipfsShell, unpinnedCids)

	// Write final statuses at the end
	defer writePinStatuses(statusPath)

	pageNum := 1
	for paginator.HasMorePages() {
		// Pagination is stateful in order to keep track of position and so should not be placed in a goroutine
		page, err := nextPage(paginator)
		if err != nil { // We've retried and still failed, exit the loop.
			log.Printf("page error: %s", err)
			break
		}
		log.Printf("retrieved page: %d", pageNum)
		cids := cidsFromPage(page)
		if len(cids) > 0 {
			pinCids(ipfsShell, cids)
		} else {
			log.Printf("no CIDs found on page: %d", pageNum)
		}
		if (pageNum % PagesBeforeSleep) == 0 {
			// Sleep before pulling more pages to avoid S3 throttling
			time.Sleep(PaginationSleep)
		}
		pageNum++
	}
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
		pageCtx, pageCancel := context.WithTimeout(ctx, RequestTimeout)
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
	for i := 0; i < NumRequestRetries; i++ {
		// Ref: https://stackoverflow.com/questions/45617758/defer-in-the-loop-what-will-be-better
		if page, err := nextFn(); err == nil {
			return page, nil
		}
		time.Sleep(time.Duration(math.Pow(2, float64(i))) * RetryDelay) // exponential backoff
	}
	return nil, errors.New("maximum retries exceeded")
}

func cidsFromPage(page *s3.ListObjectsV2Output) []string {
	cids := make([]string, 0, page.KeyCount)
	for _, object := range page.Contents {
		if object.Size != 0 { // filter out directories
			foundCount++
			// Convert file name to CID
			key := aws.ToString(object.Key)
			_, name := path.Split(key)
			cid, err := blockToCid(name)
			if err == nil {
				convertedCount++
				log.Printf("conv:key=%s,cid=%s", key, cid)
				cids = append(cids, cid)
			} else {
				log.Printf("fail:key=%s,err:%s", key, err)
			}
		}
	}
	return cids
}

func pinCids(ipfsShell *shell.Shell, cids []string) {
	for _, cid := range cids {
		// Ref: https://github.com/golang/go/wiki/CommonMistakes#using-goroutines-on-loop-iterator-variables
		cidToPin := cid
		// Pin the CID if it wasn't in the status map, or it was but marked unpinned.
		if pinned, found := pinMap.Load(cidToPin); !found || !pinned.(bool) {
			// Pin requests are standalone and can be in a goroutine for parallelism
			go func() {
				err := pinCid(ipfsShell, cidToPin)
				if err == nil {
					log.Printf("pinned: %s", cidToPin)
					pinMap.Store(cidToPin, true)
				} else {
					log.Printf("unpinned: %s, err=%s", cidToPin, err)
					pinMap.Store(cidToPin, false)
				}
			}()
		}
	}
}

func pinCid(ipfsShell *shell.Shell, cid string) error {
	pinFn := func() error {
		// Use a new child context with timeout for each pin request attempt
		pinCtx, pinCancel := context.WithTimeout(ctx, RequestTimeout)
		defer pinCancel()

		err := ipfsShell.Request("pin/add", cid).Option("recursive", false).Exec(pinCtx, nil)
		select {
		case <-pinCtx.Done():
			log.Printf("pin failed: %s", pinCtx.Err())
			return pinCtx.Err()
		default:
			// Fall through
		}
		return err
	}
	for i := 0; i < NumRequestRetries; i++ {
		if err := pinFn(); err == nil {
			return nil
		}
		time.Sleep(time.Duration(math.Pow(2, float64(i))) * RetryDelay) // exponential backoff
	}
	return errors.New("maximum retries exceeded")
}

func readPinStatuses(statusPath string) ([]string, []string) {
	pinnedCids := readPinStatus(statusPath + "/" + PinnedFilename, true)
	unpinnedCids := readPinStatus(statusPath + "/" + UnpinnedFilename, false)
	log.Printf("read pinned=%d, unpinned=%d", len(pinnedCids), len(unpinnedCids))
	return pinnedCids, unpinnedCids
}

func readPinStatus(path string, status bool) []string {
	cids := make([]string, 0)
	f, err := os.OpenFile(path, os.O_RDONLY | os.O_CREATE, 0755)
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

func writePinStatuses(path string) {
	pinned, err := os.OpenFile(path + "/" + PinnedFilename, os.O_WRONLY | os.O_TRUNC, 0755)
	defer func(p *os.File) { _ = p.Close() }(pinned)
	if err != nil {
		log.Printf("pinned create error: %s", err)
		return
	}
	unpinned, err := os.OpenFile(path + "/" + UnpinnedFilename, os.O_WRONLY | os.O_TRUNC, 0755)
	defer func(u *os.File) { _ = u.Close() }(unpinned)
	if err != nil {
		log.Printf("unpinned create error: %s", err)
		return
	}

	pinMap.Range(func(key, value interface{}) bool {
		// Ignore errors writing individual CIDs to file
		if value.(bool) {
			_, _ = fmt.Fprintln(pinned, key.(string))
			pinnedCount++
		} else {
			_, _ = fmt.Fprintln(unpinned, key.(string))
			unpinnedCount++
		}
		return true
	})
	log.Printf("%d found, %d converted, %d pinned, %d unpinned", foundCount, convertedCount, pinnedCount, unpinnedCount)
}
