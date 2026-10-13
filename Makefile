.PHONY: build run fmt vet test

build:
	mkdir -p bin
	go build -o bin/ai-gateway ./cmd/gateway

run:
	go run ./cmd/gateway -config configs/gateway.example.json

fmt:
	gofmt -w $$(find cmd internal -name '*.go')

vet:
	go vet ./...

test:
	go test -race ./...
