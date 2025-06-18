build:
	go build -o TinySQL ./cmd

run: build 
	./TinySQL

test: 
	go test ./internal/db/ -v