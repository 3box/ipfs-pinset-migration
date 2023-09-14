package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	PagesBeforeSleep     = 5 // Up to 5000 entries given a default page size of 1000
	PaginationSleep      = 1 * time.Second
	PaginationRetryDelay = 250 * time.Millisecond
	PaginationTimeout    = 3 * time.Second
	NumPaginationRetries = 3
)

type Migrate struct {
	client *s3.Client
	bucket string
	prefix string
	dest   string
}

func main() {
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion("eu-west-1"))
	if err != nil {
		log.Fatal(err)
	}
	Migrate{
		s3.NewFromConfig(cfg),
		"ceramic-prod-ex-gateway.replica",
		"ipfs/blocks",
		"migration-prod-gw/blocks",
	}.migrate()
}

func (m Migrate) migrate() {
	paginator := s3.NewListObjectsV2Paginator(m.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(m.bucket),
		Prefix: aws.String(m.prefix),
	})
	pageNum := 1
	itemCount := 0
	blockCount := 0
	nonBlockCount := 0
	unknown := 0
	fetchCount := 0
	for paginator.HasMorePages() {
		// Pagination is stateful in order to keep track of position and so should not be placed in a goroutine
		page, err := nextPage(paginator)
		if err != nil { // We've retried and still failed, exit the loop.
			log.Printf("pagination failed: %s", err)
			break
		}
		wg := sync.WaitGroup{}
		for _, item := range page.Contents {
			key := *item.Key
			parts := strings.Split(key, "/")
			if len(parts) == 4 {
				if !strings.Contains(parts[3], "CIQ") {
					unknown++
				}
				blockCount++
				if _, err := os.Stat(fmt.Sprintf("%s/%s", m.dest, strings.SplitN(*item.Key, "/", 3)[2])); errors.Is(err, os.ErrNotExist) {
					wg.Add(1)
					go func() {
						defer wg.Done()
						m.getObject(key)
						fetchCount++
					}()
				}
			} else {
				nonBlockCount++
			}
		}
		itemCount += int(page.KeyCount)
		wg.Wait()
		log.Printf("retrieved page: num=%d, ct=%d, totCt=%d, bk=%d, nbk=%d, ft=%d, unk=%d", pageNum, page.KeyCount, itemCount, blockCount, nonBlockCount, fetchCount, unknown)
		pageNum++
	}
}

func nextPage(paginator *s3.ListObjectsV2Paginator) (*s3.ListObjectsV2Output, error) {
	nextFn := func() (*s3.ListObjectsV2Output, error) {
		// Use a new child context with timeout for each page retrieval attempt
		pageCtx, pageCancel := context.WithTimeout(context.Background(), PaginationTimeout)
		defer pageCancel()

		type pageResult struct {
			page *s3.ListObjectsV2Output
			err  error
		}
		// Buffered channel with a single slot
		ch := make(chan pageResult, 1)

		wg := sync.WaitGroup{}
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
	}
	return nil, errors.New("maximum retries exceeded")
}

func (m Migrate) getObject(key string) error {
	result, err := m.client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(m.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		log.Printf("Couldn't get object %v:%v. Here's why: %v\n", m.bucket, key, err)
		return err
	}
	defer result.Body.Close()
	parts := strings.Split(key, "/")
	dirPath := m.dest + "/" + parts[2]
	os.MkdirAll(dirPath, os.ModePerm)
	filename := dirPath + "/" + parts[3]
	file, err := os.Create(filename)
	if err != nil {
		log.Printf("Couldn't create file %v. Here's why: %v\n", filename, err)
		return err
	}
	defer file.Close()
	body, err := io.ReadAll(result.Body)
	if err != nil {
		log.Printf("Couldn't read object body from %v. Here's why: %v\n", key, err)
	}
	_, err = file.Write(body)
	return err
}
