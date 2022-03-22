package main

import (
	"fmt"
	"os"

	migrate "github.com/3box/ipfs-pinset-migration/migrate"
	flag "github.com/ogier/pflag"
)

var (
	bucket     string
	pinsPrefix string
	ipfsApiUrl string
	statusPath string
)

func main() {
	flag.Parse()
	if flag.NFlag() < 2 {
		fmt.Printf("Usage: %s [options]\n", os.Args[0])
		fmt.Println("Options:")
		flag.PrintDefaults()
		os.Exit(1)
	}
	migrate.Migrate(bucket, pinsPrefix, ipfsApiUrl, statusPath)
}

func init() {
	flag.StringVarP(&bucket, "bucket", "b", "", "S3 bucket name")
	flag.StringVarP(&pinsPrefix, "prefix", "p", "", "S3 bucket prefix for pins")
	flag.StringVarP(&ipfsApiUrl, "ipfs", "i", "localhost:5001", "IPFS API url")
	flag.StringVarP(&statusPath, "status", "s", "/tmp", "path for status files")
}
