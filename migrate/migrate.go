package migrate

import (
	"context"
	"log"
	"path"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	ds "github.com/ipfs/go-datastore"
	shell "github.com/ipfs/go-ipfs-api"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
)

func Migrate(bucket, prefix, ipfsUrl string) {
	cfg := configureAWS()
	s3Client := s3.NewFromConfig(cfg)

	output, err := s3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: 100,
	})

	if err != nil {
		log.Fatal(err)
	}

	sh := shell.NewShell(ipfsUrl)

	for _, object := range output.Contents {
		if object.Size != 0 { // filter out directories
			log.Printf("key=%s", aws.ToString(object.Key))
			_, file := path.Split(aws.ToString(object.Key))
			multihash, err := blockToMultihash(file)
			log.Printf("b58=%s", multihash)
			if err != nil {
				log.Printf("Failed to convert key %v\n", file)
			} else {
				// err := sh.Pin(multihash) // this defaults recursive true
				err := sh.Request("pin/add", multihash).Option("recursive", false).Exec(context.Background(), nil)
				if err != nil {
					log.Printf("error: %s", err)
				} else {
					log.Printf("\nSuccessfully pinned %s\n", multihash)
				}
			}
		}
	}
}

func configureAWS() aws.Config {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatal(err)
	}
	return cfg
}

func blockToMultihash(key string) (string, error) {
	cid, err := dshelp.DsKeyToCidV1(ds.NewKey(key), 0x85) // assuming dag-jose
	return cid.String(), err
	// otherwise use cidv0
	// mh, err := dshelp.DsKeyToMultihash(ds.NewKey(key))
	// return mh.B58String(), err
}
