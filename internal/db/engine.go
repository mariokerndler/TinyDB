package db

import "strings"

type Engine struct {
	wal  *WAL
	tree *BPlusTree
}

func NewEngine(logPath string) *Engine {
	wal := NewWAL(logPath)
	tree := NewBTree()

	// Recover data from WAL
	entries, err := wal.Replay()
	if err != nil {
		panic("Failed to replay WAL: " + err.Error())
	}

	for _, entry := range entries {
		tree.Insert(entry[0], entry[1])
	}

	return &Engine{wal, tree}
}

func (e *Engine) Execute(cmd string) string {
	parts := strings.Fields(cmd)
	if len(parts) == 0 {
		return "Empty command"
	}

	switch strings.ToUpper(parts[0]) {
	case "SET":
		if len(parts) != 3 {
			return "Usage: SET key value"
		}
		e.tree.Insert(parts[1], parts[2])
		e.wal.Append(parts[1], parts[2])
		return "OK"

	case "GET":
		if len(parts) != 2 {
			return "Usage: GET key"
		}
		if val, ok := e.tree.Get(parts[1]); ok {
			return val
		}
		return "Key not found"

	default:
		return "Unknown command"
	}
}
