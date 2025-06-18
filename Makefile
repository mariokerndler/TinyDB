build:
	go build -o TinySQL ./cmd

run: build 
	./TinySQL