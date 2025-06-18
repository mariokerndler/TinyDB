package db

import (
	"fmt"
	"strings"
)

type Engine struct {
	wal  *WAL
	tree *BPlusTree
}

func NewEngine(logPath string) *Engine {
	wal := NewWAL(logPath)
	tree := NewBPlusTree()

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

	case "DELETE":
		if len(parts) != 2 {
			return "Usage: DELETE key"
		}
		e.tree.Delete(parts[1])
		e.wal.Delete(parts[1])
		// Optionally, you could log deletion in the WAL (not implemented here)
		return "Deleted"

	case "SCAN":
		if len(parts) != 3 {
			return "Usage: SCAN startKey endKey"
		}
		results := e.tree.RangeQuery(parts[1], parts[2])
		if len(results) == 0 {
			return "No results"
		}
		var sb strings.Builder
		for k, v := range results {
			sb.WriteString(fmt.Sprintf("%s: %s\n", k, v))
		}
		return strings.TrimRight(sb.String(), "\n")

	default:
		return "Unknown command"
	}
}
