package db

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

type Engine struct {
	wal    *WAL
	tables map[string]*BPlusTree

	// Transaction management
	mu              sync.Mutex // Global mutex for simplified concurrency control
	currentTxID     string
	txChanges       map[string]map[string]string   // table -> key -> value (for SET/INSERT/UPDATE)
	txDeletes       map[string]map[string]struct{} // table -> key -> {} (for DELETE)
	txDroppedTables map[string]struct{}            // table -> {} (for DROP)
}

func NewEngine(logPath string) *Engine {
	wal := NewWAL(logPath)
	engine := &Engine{
		wal:             wal,
		tables:          make(map[string]*BPlusTree),
		txChanges:       make(map[string]map[string]string),
		txDeletes:       make(map[string]map[string]struct{}),
		txDroppedTables: make(map[string]struct{}),
	}

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
	e.mu.Lock()
	defer e.mu.Unlock()

	stmt, err := Parse(cmd)
	if err != nil {
		return "Parse error: " + err.Error()
	}

	// Handle transaction control statements first
	switch s := stmt.(type) {
	case *BeginStatement:
		_ = s // Acknowledge 's' is declared but not directly used
		if e.currentTxID != "" {
			return "Error: A transaction is already active. Commit or rollback the current transaction first."
		}
		e.currentTxID = fmt.Sprintf("tx_%d", time.Now().UnixNano())
		e.txChanges = make(map[string]map[string]string)
		e.txDeletes = make(map[string]map[string]struct{})
		e.txDroppedTables = make(map[string]struct{})
		e.wal.BeginTx(e.currentTxID) // Updated WAL call
		return "Transaction started: " + e.currentTxID

	case *CommitStatement:
		_ = s // Acknowledge 's' is declared but not directly used
		if e.currentTxID == "" {
			return "Error: No active transaction to commit."
		}
		txIDToCommit := e.currentTxID

		for tableName := range e.txDroppedTables {
			delete(e.tables, tableName)
			e.wal.DropTable(txIDToCommit, tableName) // Updated WAL call
		}

		for tableName, kvs := range e.txChanges {
			tree, ok := e.tables[tableName]
			if !ok {
				tree = NewBPlusTree()
				e.tables[tableName] = tree
			}
			for key, value := range kvs {
				if _, exists := tree.Get(key); exists {
					tree.Update(key, value)
				} else {
					tree.Insert(key, value)
				}
				e.wal.Append(txIDToCommit, tableName, key, value) // Updated WAL call
			}
		}

		for tableName, keysToDelete := range e.txDeletes {
			tree, ok := e.tables[tableName]
			if !ok {
				continue
			}
			for key := range keysToDelete {
				if tree.Delete(key) {
					e.wal.Delete(txIDToCommit, tableName, key) // Updated WAL call
				}
			}
		}

		e.wal.CommitTx(txIDToCommit) // Updated WAL call
		e.currentTxID = ""
		e.txChanges = nil
		e.txDeletes = nil
		e.txDroppedTables = nil
		return fmt.Sprintf("Transaction %s committed.", txIDToCommit)

	case *RollbackStatement:
		_ = s // Acknowledge 's' is declared but not directly used
		if e.currentTxID == "" {
			return "Error: No active transaction to rollback."
		}
		txIDToRollback := e.currentTxID

		e.currentTxID = ""
		e.txChanges = nil
		e.txDeletes = nil
		e.txDroppedTables = nil
		e.wal.RollbackTx(txIDToRollback) // Updated WAL call
		return fmt.Sprintf("Transaction %s rolled back.", txIDToRollback)

	default:
		if e.currentTxID == "" {
			return e.executeAutocommit(stmt)
		} else {
			return e.executeInTransaction(stmt)
		}
	}
}

func (e *Engine) executeAutocommit(stmt Statement) string {
	switch s := stmt.(type) {
	case *InsertStatement:
		tree, ok := e.tables[s.Table]
		if !ok {
			tree = NewBPlusTree()
			e.tables[s.Table] = tree
		}
		insertedCount := 0
		for _, kv := range s.Values {
			didInsert := tree.Insert(kv.Key, kv.Value)
			if didInsert {
				e.wal.Append("", s.Table, kv.Key, kv.Value) // Updated WAL call (empty txID)
				insertedCount++
			}

		}
		if insertedCount == 0 && len(s.Values) > 0 {
			return "No new keys inserted (they might already exist)"
		}
		return fmt.Sprintf("Inserted %d key(s) into table '%s'", insertedCount, s.Table)

	case *SelectStatement:
		tree, ok := e.tables[s.Table]
		if !ok {
			return fmt.Sprintf("Table '%s' not found", s.Table)
		}
		var sb strings.Builder
		if len(s.Keys) > 0 {
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
			results := tree.RangeQuery("", "")
			if len(results) == 0 {
				return "No results"
			}
			keys := make([]string, 0, len(results))
			for k := range results {
				keys = append(keys, k)
			}
			sort.Strings(keys)

			for _, k := range keys {
				sb.WriteString(fmt.Sprintf("%s: %s\n", k, results[k]))
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
			if tree.Delete(key) {
				e.wal.Delete("", s.Table, key) // Updated WAL call (empty txID)
				deletedCount++
			}
		}

		if deletedCount > 0 {
			return fmt.Sprintf("Deleted %d key(s) from table '%s'", deletedCount, s.Table)
		}
		return "No key(s) found to delete in table '" + s.Table + "'"

	case *DropStatement:
		_, ok := e.tables[s.Table]
		if !ok {
			return fmt.Sprintf("Table '%s' not found", s.Table)
		}
		delete(e.tables, s.Table)
		e.wal.DropTable("", s.Table) // Updated WAL call (empty txID)
		return fmt.Sprintf("Table '%s' dropped", s.Table)

	case *UpdateStatement:
		tree, ok := e.tables[s.Table]
		if !ok {
			return fmt.Sprintf("Table '%s' not found", s.Table)
		}
		updatedCount := 0
		for _, kv := range s.Values {
			if tree.Update(kv.Key, kv.Value) {
				e.wal.Append("", s.Table, kv.Key, kv.Value) // Updated WAL call (empty txID)
				updatedCount++
			}
		}
		if updatedCount > 0 {
			return fmt.Sprintf("Updated %d key(s) in table '%s'", updatedCount, s.Table)
		}
		return "No keys found to update"

	default:
		return fmt.Errorf("unsupported statement in autocommit mode: %s", stmt.StmtType()).Error()
	}
}

func (e *Engine) executeInTransaction(stmt Statement) string {
	switch s := stmt.(type) {
	case *InsertStatement:
		if _, droppedInTx := e.txDroppedTables[s.Table]; droppedInTx {
			return fmt.Sprintf("Table '%s' marked for drop within this transaction, cannot insert into it", s.Table)
		}

		if _, ok := e.txChanges[s.Table]; !ok {
			e.txChanges[s.Table] = make(map[string]string)
		}

		insertedOrUpdatedCount := 0
		for _, kv := range s.Values { // kv is correctly defined here for each iteration
			if _, ok := e.txDeletes[s.Table]; ok {
				delete(e.txDeletes[s.Table], kv.Key)
			}
			// Safely check if the table exists in the main engine's tables for 'existsInMain'
			var existsInMain bool
			if tree, ok := e.tables[s.Table]; ok {
				_, existsInMain = tree.Get(kv.Key)
			} else {
				existsInMain = false // Table does not exist in main tables
			}

			_, existsInTxChanges := e.txChanges[s.Table][kv.Key]

			if !existsInMain && !existsInTxChanges {
				insertedOrUpdatedCount++
			} else if existsInTxChanges {
				insertedOrUpdatedCount++
			} else if existsInMain {
				insertedOrUpdatedCount++
			}

			e.txChanges[s.Table][kv.Key] = kv.Value
		}
		if insertedOrUpdatedCount == 0 && len(s.Values) > 0 {
			return "No new keys inserted or values updated (they might already exist with the same value)"
		}
		return fmt.Sprintf("Buffered %d key(s) for insert/update into table '%s'", len(s.Values), s.Table)

	case *SelectStatement:
		if _, droppedInTx := e.txDroppedTables[s.Table]; droppedInTx {
			return fmt.Sprintf("Table '%s' dropped within this transaction", s.Table)
		}

		type combinedEntry struct {
			Value  string
			FromTx bool
		}
		combinedData := make(map[string]combinedEntry)

		tree, ok := e.tables[s.Table]
		if ok {
			allKeysValues := tree.RangeQuery("", "")
			for k, v := range allKeysValues {
				combinedData[k] = combinedEntry{Value: v, FromTx: false}
			}
		}

		if delKeys, ok := e.txDeletes[s.Table]; ok {
			for key := range delKeys {
				delete(combinedData, key)
			}
		}

		if txKVs, ok := e.txChanges[s.Table]; ok {
			for k, v := range txKVs {
				combinedData[k] = combinedEntry{Value: v, FromTx: true}
			}
		}

		var sb strings.Builder
		if len(s.Keys) > 0 {
			foundResults := false
			for _, key := range s.Keys {
				if entry, ok := combinedData[key]; ok {
					if entry.FromTx {
						sb.WriteString(fmt.Sprintf("%s: [%s] %s\n", key, e.currentTxID, entry.Value))
					} else {
						sb.WriteString(fmt.Sprintf("%s: %s\n", key, entry.Value))
					}
					foundResults = true
				}
			}
			if !foundResults {
				return "No results"
			}
			return strings.TrimRight(sb.String(), "\n")
		} else {
			if len(combinedData) == 0 {
				return "No results"
			}
			keys := make([]string, 0, len(combinedData))
			for k := range combinedData {
				keys = append(keys, k)
			}
			sort.Strings(keys)

			for _, k := range keys {
				entry := combinedData[k]
				if entry.FromTx {
					sb.WriteString(fmt.Sprintf("%s: [%s] %s\n", k, e.currentTxID, entry.Value))
				} else {
					sb.WriteString(fmt.Sprintf("%s: %s\n", k, entry.Value))
				}
			}
			return strings.TrimRight(sb.String(), "\n")
		}

	case *DeleteStatement:
		if _, droppedInTx := e.txDroppedTables[s.Table]; droppedInTx {
			return fmt.Sprintf("Table '%s' marked for drop within this transaction, cannot delete from it", s.Table)
		}
		if _, ok := e.tables[s.Table]; !ok {
			if _, ok := e.txChanges[s.Table]; !ok {
				return fmt.Sprintf("Table '%s' not found", s.Table)
			}
		}

		if _, ok := e.txDeletes[s.Table]; !ok {
			e.txDeletes[s.Table] = make(map[string]struct{})
		}
		deletedCount := 0
		for _, key := range s.Keys {
			var existsInMain bool
			if tree, ok := e.tables[s.Table]; ok {
				_, existsInMain = tree.Get(key)
			} else {
				existsInMain = false
			}

			_, existsInTxChanges := e.txChanges[s.Table][key]

			if existsInMain || existsInTxChanges {
				e.txDeletes[s.Table][key] = struct{}{}
				if existsInTxChanges {
					delete(e.txChanges[s.Table], key)
				}
				deletedCount++
			}
		}
		if deletedCount > 0 {
			return fmt.Sprintf("Buffered %d key(s) for deletion from table '%s'", deletedCount, s.Table)
		}
		return "No key(s) found to delete in table '" + s.Table + "'"

	case *DropStatement:
		if _, ok := e.tables[s.Table]; !ok {
			if _, createdInTx := e.txChanges[s.Table]; !createdInTx {
				return fmt.Sprintf("Table '%s' not found", s.Table)
			}
		}

		e.txDroppedTables[s.Table] = struct{}{}
		delete(e.txChanges, s.Table)
		delete(e.txDeletes, s.Table)
		return fmt.Sprintf("Buffered DROP for table '%s'", s.Table)

	case *UpdateStatement:
		if _, droppedInTx := e.txDroppedTables[s.Table]; droppedInTx {
			return fmt.Sprintf("Table '%s' marked for drop within this transaction, cannot update it", s.Table)
		}
		if _, ok := e.tables[s.Table]; !ok {
			if _, ok := e.txChanges[s.Table]; !ok {
				return fmt.Sprintf("Table '%s' not found", s.Table)
			}
		}

		if _, ok := e.txChanges[s.Table]; !ok {
			e.txChanges[s.Table] = make(map[string]string)
		}

		updatedCount := 0
		for _, kv := range s.Values {
			var existsInMain bool
			if tree, ok := e.tables[s.Table]; ok {
				_, existsInMain = tree.Get(kv.Key)
			} else {
				existsInMain = false
			}

			_, existsInTxChanges := e.txChanges[s.Table][kv.Key]
			_, existsInTxDeletes := e.txDeletes[s.Table][kv.Key]

			if existsInMain || existsInTxChanges || existsInTxDeletes {
				updatedCount++
				if existsInTxDeletes {
					delete(e.txDeletes[s.Table], kv.Key)
				}
				e.txChanges[s.Table][kv.Key] = kv.Value
			}
		}
		if updatedCount > 0 {
			return fmt.Sprintf("Buffered %d key(s) for update in table '%s'", updatedCount, s.Table)
		}
		return "No keys found to update"

	default:
		return fmt.Errorf("unsupported statement in transaction mode: %s", stmt.StmtType()).Error()
	}
}
