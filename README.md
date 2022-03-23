# ipfs-pinset-migration

Migrate pins from S3 bucket to go-ipfs

## Usage

```sh
cd cmd/ipfs-pinset-migration
go run main.go --bucket=ceramic-dev-node --prefix=ipfs/pins --ipfs=localhost:5001 --logPath=/tmp
```

With Docker
```sh
docker build . -t ipfs-pinset-migration

docker run \
-e AWS_REGION= \
-e AWS_ACCESS_KEY_ID= \
-e AWS_SECRET_ACCESS_KEY= \
-e S3_PINSET_BUCKET_NAME= \
-e S3_PINSET_BUCKET_PREFIX= \
-e IPFS_API_URL= \
-e LOG_PATH = \
ipfs-pinset-migration
```
