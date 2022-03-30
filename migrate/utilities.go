package migrate

import (
	"log"
	"math"
	"path"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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
