package migrate

import (
	"context"
	"fmt"
	"log"
	"path"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	ds "github.com/ipfs/go-datastore"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
)

func Migrate(bucket, prefix string) {
	cfg := configureAWS()
	s3Client := s3.NewFromConfig(cfg)

	output, err := s3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})

	if err != nil {
		log.Fatal(err)
	}

	log.Println("First page of files:")
	for _, object := range output.Contents {
		if object.Size != 0 {
			log.Printf("key=%s", aws.ToString(object.Key))
			_, file := path.Split(aws.ToString(object.Key))
			log.Printf("b58=%s", blockToMultihash(file))
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

func blockToMultihash(key string) string {
	mh, err := dshelp.DsKeyToMultihash(ds.NewKey(key))
	if err != nil {
		fmt.Printf("Failed to convert key %v\n", key)
	}
	return mh.B58String()
}
