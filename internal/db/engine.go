package db

import (
	"fmt"
	"strings"
)

type Engine struct {
	wal    *WAL
	tables map[string]*BPlusTree
}

func NewEngine(logPath string) *Engine {
	wal := NewWAL(logPath)
	engine := &Engine{
		wal:    wal,
		tables: make(map[string]*BPlusTree),
	}

	// Recover data for all tables from WAL
	tablesData, err := wal.Replay()
	if err != nil {
		panic("Failed to replay WAL: " + err.Error())
	}

	for tableName, entries := range tablesData {
		tree := NewBPlusTree()
		for _, entry := range entries {
			tree.Insert(entry[0], entry[1])
		}
		engine.tables[tableName] = tree
	}
	return engine
}

func (e *Engine) Execute(cmd string) string {
	stmt, err := Parse(cmd)
	if err != nil {
		return "Parse error: " + err.Error()
	}

	switch s := stmt.(type) {
	case *InsertStatement:
		// Get or create the table's BPlusTree
		tree, ok := e.tables[s.Table]
		if !ok {
			tree = NewBPlusTree()
			e.tables[s.Table] = tree
		}

		insertedCount := 0
		for _, kv := range s.Values {
			if tree.Insert(kv.Key, kv.Value) { // Insert only if key is new
				e.wal.Append(s.Table, kv.Key, kv.Value) // Log with table name
				insertedCount++
			}
		}
		if insertedCount > 0 {
			return fmt.Sprintf("Inserted %d key(s) into table '%s'", insertedCount, s.Table)
		}
		return "No new keys inserted (they might already exist)"

	case *UpdateStatement: // New case for UPDATE
		tree, ok := e.tables[s.Table]
		if !ok {
			return fmt.Sprintf("Table '%s' not found", s.Table)
		}

		updatedCount := 0
		for _, kv := range s.Values {
			if tree.Update(kv.Key, kv.Value) { // Update only if key exists
				e.wal.Append(s.Table, kv.Key, kv.Value) // Log the update as a SET operation
				updatedCount++
			}
		}
		if updatedCount > 0 {
			return fmt.Sprintf("Updated %d key(s) in table '%s'", updatedCount, s.Table)
		}
		return "No keys found to update"

	case *SelectStatement:
		tree, ok := e.tables[s.Table]
		if !ok {
			return fmt.Sprintf("Table '%s' not found", s.Table)
		}

		var sb strings.Builder
		if len(s.Keys) > 0 {
			// Specific keys selected
			foundResults := false
			for _, key := range s.Keys {
				val, ok := tree.Get(key)
				if ok {
					sb.WriteString(fmt.Sprintf("%s: %s\n", key, val))
					foundResults = true
				}
			}
			if !foundResults {
				return "No results"
			}
			return strings.TrimRight(sb.String(), "\n")
		} else {
			// SELECT * - scan all
			results := tree.RangeQuery("", "")
			if len(results) == 0 {
				return "No results"
			}
			for k, v := range results {
				sb.WriteString(fmt.Sprintf("%s: %s\n", k, v))
			}
			return strings.TrimRight(sb.String(), "\n")
		}

	case *DeleteStatement:
		tree, ok := e.tables[s.Table]
		if !ok {
			return fmt.Sprintf("Table '%s' not found", s.Table)
		}

		deletedCount := 0
		for _, key := range s.Keys {
			if tree.Delete(key) { // BPlusTree.Delete now returns bool
				e.wal.Delete(s.Table, key) // Log with table name
				deletedCount++
			}
		}

		if deletedCount > 0 {
			return fmt.Sprintf("Deleted %d key(s) from table '%s'", deletedCount, s.Table)
		}
		return fmt.Sprintf("No key(s) found to delete in table '%s'", s.Table)

	case *DropStatement:
		_, ok := e.tables[s.Table]
		if !ok {
			return fmt.Sprintf("Table '%s' not found", s.Table)
		}
		delete(e.tables, s.Table) // Remove the table from the in-memory map
		e.wal.DropTable(s.Table)  // Log the drop operation
		return fmt.Sprintf("Table '%s' dropped", s.Table)

	default:
		return "Unsupported statement type"
	}
}
