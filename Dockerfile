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

ENTRYPOINT ["ipfs-pinset-migration"]
