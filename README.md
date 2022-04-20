# ipfs-pinset-migration

Migrate pins from S3 bucket to go-ipfs

## Usage

### CLI

S3:
```sh
cd cmd/ipfs-pinset-migration
go run main.go s3 --bucket=ceramic-dev-node --prefix=ipfs/pins --ipfs=localhost:5001 --logPath=/tmp
```

Filesystem:
```sh
cd cmd/ipfs-pinset-migration
go run main.go fs --path=/root/.jsipfs/pins --ipfs=localhost:5001 --logPath=/tmp
```

### Docker:

S3:
```sh
docker build . -t ipfs-pinset-migration

docker run \
-e AWS_REGION= \
-e AWS_ACCESS_KEY_ID= \
-e AWS_SECRET_ACCESS_KEY= \
ipfs-pinset-migration s3 --bucket=S3_BUCKET_NAME --prefix=S3_BUCKET_PREFIX --ipfs-api-url=IPFS_API_URL --log-path=LOG_PATH
```

Filesystem:
```sh
docker build . -t ipfs-pinset-migration

docker run ipfs-pinset-migration fs --path=FS_PATH --ipfs-api-url=IPFS_API_URL --log-path=LOG_PATH
```
