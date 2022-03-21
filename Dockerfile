FROM golang:1.16-alpine

WORKDIR /app

# Download and cache dependencies

COPY migrate/go.* ./migrate/
COPY cmd/ipfs-pinset-migration/go.* ./cmd/ipfs-pinset-migration/

RUN cd cmd/ipfs-pinset-migration && go mod download

# Build and cache binary

COPY migrate/*.go ./migrate/
COPY cmd/ipfs-pinset-migration/*.go ./cmd/ipfs-pinset-migration/

RUN cd ./cmd/ipfs-pinset-migration && go build -o /usr/local/bin/ipfs-pinset-migration

# Execute

CMD ipfs-pinset-migration --bucket=$S3_PINSET_BUCKET_NAME --prefix=$S3_PINSET_BUCKET_PREFIX --ipfs=$IPFS_API_URL
