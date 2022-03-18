# ipfs-pinset-migration

Migrate pins from S3 bucket to go-ipfs

## Usage

```sh
cd cmd/ipfs-pinset-migration
go run main.go --bucket=ceramic-dev-node --prefix=ipfs/pins --ipfs=localhost:5001
```
