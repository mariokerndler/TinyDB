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

func (w *WAL) Replay() ([][2]string, error) {
	f, err := os.Open(w.path)
	if err != nil {
		return nil, err
	}

	defer f.Close()

	var entries [][2]string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) != 3 || strings.ToUpper(parts[0]) != "SET" {
			continue
		}
		entries = append(entries, [2]string{parts[1], parts[2]})
	}

	return entries, scanner.Err()
}
