.PHONY: build run fmt

build:
	mkdir -p bin
	go build -o bin/ai-gateway ./cmd/gateway

run:
	go run ./cmd/gateway -config configs/gateway.example.json

fmt:
	gofmt -w $$(find cmd internal -name '*.go')
