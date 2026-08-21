.PHONY: fmt vet test build check run

fmt:
	go fmt ./...

vet:
	go vet ./...

test:
	go test ./...

build:
	go build ./...

check: fmt vet test build

run:
	go run ./cmd/forgeflow
