.PHONY: all build test fmt lint tidy

all: test

build:
	go build ./...

test:
	go test ./...

fmt:
	go fmt ./...

lint:
	golangci-lint run

tidy:
	go mod tidy
