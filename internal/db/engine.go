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
	stmt, err := Parse(cmd)
	if err != nil {
		return "Parse error: " + err.Error()
	}

	switch s := stmt.(type) {
	case *InsertStatement:
		for _, kv := range s.Values {
			e.tree.Insert(kv.Key, kv.Value)
			e.wal.Append(kv.Key, kv.Value)
		}
		return "OK"

	case *SelectStatement:
		if s.Where == nil {
			// No WHERE clause - scan all
			results := e.tree.RangeQuery("", "")
			if len(results) == 0 {
				return "No results"
			}
			var sb strings.Builder
			for k, v := range results {
				sb.WriteString(fmt.Sprintf("%s: %s\n", k, v))
			}
			return strings.TrimRight(sb.String(), "\n")
		} else {
			// WHERE clause present - exact match for now
			val, ok := e.tree.Get(s.Where.Value)
			if ok {
				return val
			}
			return "Key not found"
		}

	case *DeleteStatement:
		e.tree.Delete(s.Value)
		e.wal.Delete(s.Value)
		return "Deleted"

	case *DropStatement:
		results := e.tree.RangeQuery("", "")
		if len(results) == 0 {
			return "Nothing to drop"
		}

		for k, _ := range results {
			e.tree.Delete(k)
			e.wal.Delete(k)
		}
		return "Table dropped"

	default:
		return "Unsupported statement type"
	}
}
