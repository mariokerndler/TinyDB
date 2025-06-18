package db

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type WAL struct {
	file *os.File
	path string
}

func NewWAL(path string) *WAL {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}

	return &WAL{file: f, path: path}
}

func (w *WAL) Append(key, value string) {
	fmt.Fprintf(w.file, "SET %s %s\n", key, value)
}

func (w *WAL) Delete(key string) {
	fmt.Fprintf(w.file, "DELETE %s\n", key)
}

func (w *WAL) Replay() ([][2]string, error) {
	f, err := os.Open(w.path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	entries := make(map[string]string)
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}

		switch strings.ToUpper(parts[0]) {
		case "SET":
			if len(parts) == 3 {
				entries[parts[1]] = parts[2]
			}
		case "DELETE":
			if len(parts) == 2 {
				delete(entries, parts[1])
			}
		}
	}

	// Convert map to slice
	var result [][2]string
	for k, v := range entries {
		result = append(result, [2]string{k, v})
	}

	return result, scanner.Err()
}
