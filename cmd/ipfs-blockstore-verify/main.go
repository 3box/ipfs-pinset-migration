package main

import (
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"time"

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
	items, err := ioutil.ReadDir("migration")
	if err != nil {
		log.Fatalf("failed to read dir: %v", err)
	}
	verifiedCount := 0
	for _, item := range items {
		if item.IsDir() {
			subItems, err := ioutil.ReadDir("migration/" + item.Name())
			if err != nil {
				log.Fatalf("failed to read dir: %s, %v", "migration/"+item.Name(), err)
			}
			for _, subItem := range subItems {
				if subItem.IsDir() {
					blocks, err := ioutil.ReadDir("migration/" + item.Name() + "/" + subItem.Name())
					if err != nil {
						log.Fatalf("failed to read dir: %s, %v", "migration/"+item.Name()+"/"+subItem.Name(), err)
					}
					for _, block := range blocks {
						if err := testObject(block.Name()); err != nil {
							log.Fatalf("failed to verify block: %s", block.Name())
						}
						verifiedCount++
					}
				}
				log.Printf("verified: %d", verifiedCount)
			}
		}
	}
}

func testObject(cid string) error {
	url := "http://localhost:5001/api/v0/block/stat?arg=b" + cid
	req, err := http.NewRequestWithContext(context.Background(), "POST", url, nil)
	if err != nil {
		log.Printf("test: error creating request: %v", err)
		return err
	}
	req.Header.Add("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("test: error submitting request: %v", err)
		return err
	}
	defer resp.Body.Close()
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("test: error reading response: %v", err)
		return err
	}
	if resp.StatusCode != http.StatusOK {
		log.Printf("error in query: %v, %s, %s", resp.StatusCode, respBody, url)
		return errors.New("test: error in response")
	}
	stat := struct {
		Key  string
		Size float32
	}{}
	if err = json.Unmarshal(respBody, &stat); err != nil {
		log.Printf("sync: error unmarshaling response: %v", err)
		return err
	}
	return nil
}
