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
)

func main() {
	flag.Parse()
	if flag.NFlag() < 2 {
		fmt.Printf("Usage: %s [options]\n", os.Args[0])
		fmt.Println("Options:")
		flag.PrintDefaults()
		os.Exit(1)
	}
	migrate.Migrate(bucket, pinsPrefix)
}

func init() {
	flag.StringVarP(&bucket, "bucket", "b", "<s3_bucket_name>", "S3 bucket name")
	flag.StringVarP(&pinsPrefix, "prefix", "p", "<s3_bucket_prefix>", "S3 bucket prefix for pins")
}
