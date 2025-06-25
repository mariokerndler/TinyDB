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

// Append logs a SET operation. txID is empty for autocommit.
func (w *WAL) Append(txID, tableName, key, value string) {
	if txID == "" {
		fmt.Fprintf(w.file, "SET %s %s %s\n", tableName, key, value) // Autocommit format
	} else {
		fmt.Fprintf(w.file, "SET %s %s %s %s\n", txID, tableName, key, value) // Transactional format
	}
}

// Delete logs a DELETE operation. txID is empty for autocommit.
func (w *WAL) Delete(txID, tableName, key string) {
	if txID == "" {
		fmt.Fprintf(w.file, "DELETE %s %s\n", tableName, key) // Autocommit format
	} else {
		fmt.Fprintf(w.file, "DELETE %s %s %s\n", txID, tableName, key) // Transactional format
	}
}

// DropTable logs a DROP TABLE operation. txID is empty for autocommit.
func (w *WAL) DropTable(txID, tableName string) {
	if txID == "" {
		fmt.Fprintf(w.file, "DROP TABLE %s\n", tableName) // Autocommit format
	} else {
		fmt.Fprintf(w.file, "DROP TABLE %s %s\n", txID, tableName) // Transactional format
	}
}

// New functions for transaction boundaries
func (w *WAL) BeginTx(txID string) {
	fmt.Fprintf(w.file, "BEGIN_TX %s\n", txID)
}

func (w *WAL) CommitTx(txID string) {
	fmt.Fprintf(w.file, "COMMIT_TX %s\n", txID)
}

func (w *WAL) RollbackTx(txID string) {
	fmt.Fprintf(w.file, "ROLLBACK_TX %s\n", txID)
}

// Replay reads the WAL and reconstructs the state of all tables.
func (w *WAL) Replay() (map[string][][2]string, error) {
	f, err := os.Open(w.path)
	if err != nil {
		if os.IsNotExist(err) {
			return make(map[string][][2]string), nil
		}
		return nil, err
	}
	defer f.Close()

	tablesData := make(map[string]map[string]string)                   // current state of tables
	activeTxChanges := make(map[string]map[string]map[string]string)   // txID -> table -> key -> value
	activeTxDeletes := make(map[string]map[string]map[string]struct{}) // txID -> table -> key -> {}
	activeTxDroppedTables := make(map[string]map[string]struct{})      // txID -> table -> {}

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}

		command := strings.ToUpper(parts[0])
		switch command {
		case "SET":
			if len(parts) == 5 { // Transactional SET: SET <txID> <table_name> <key> <value>
				txID := parts[1]
				tableName := parts[2]
				key := parts[3]
				value := parts[4]
				if _, ok := activeTxChanges[txID]; !ok {
					activeTxChanges[txID] = make(map[string]map[string]string)
				}
				if _, ok := activeTxChanges[txID][tableName]; !ok {
					activeTxChanges[txID][tableName] = make(map[string]string)
				}
				activeTxChanges[txID][tableName][key] = value
			} else if len(parts) == 4 { // Autocommit SET: SET <table_name> <key> <value>
				tableName := parts[1]
				key := parts[2]
				value := parts[3]
				if _, ok := tablesData[tableName]; !ok {
					tablesData[tableName] = make(map[string]string)
				}
				tablesData[tableName][key] = value
			}
		case "DELETE":
			if len(parts) == 4 { // Transactional DELETE: DELETE <txID> <table_name> <key>
				txID := parts[1]
				tableName := parts[2]
				key := parts[3]
				if _, ok := activeTxDeletes[txID]; !ok {
					activeTxDeletes[txID] = make(map[string]map[string]struct{})
				}
				if _, ok := activeTxDeletes[txID][tableName]; !ok {
					activeTxDeletes[txID][tableName] = make(map[string]struct{})
				}
				activeTxDeletes[txID][tableName][key] = struct{}{}
			} else if len(parts) == 3 { // Autocommit DELETE: DELETE <table_name> <key>
				tableName := parts[1]
				key := parts[2]
				if _, ok := tablesData[tableName]; ok {
					delete(tablesData[tableName], key)
				}
			}
		case "DROP":
			if len(parts) == 4 && strings.ToUpper(parts[1]) == "TABLE" { // Transactional DROP: DROP TABLE <txID> <table_name>
				txID := parts[2]
				tableName := parts[3]
				if _, ok := activeTxDroppedTables[txID]; !ok {
					activeTxDroppedTables[txID] = make(map[string]struct{})
				}
				activeTxDroppedTables[txID][tableName] = struct{}{}
			} else if len(parts) == 3 && strings.ToUpper(parts[1]) == "TABLE" { // Autocommit DROP: DROP TABLE <table_name>
				tableName := parts[2]
				delete(tablesData, tableName)
			}
		case "BEGIN_TX":
			// No action needed during replay, just marks the start
		case "COMMIT_TX":
			if len(parts) == 2 { // COMMIT_TX <txID>
				txID := parts[1]
				// Apply buffered changes for this transaction to tablesData
				if changes, ok := activeTxChanges[txID]; ok {
					for tableName, kvs := range changes {
						if _, ok := tablesData[tableName]; !ok {
							tablesData[tableName] = make(map[string]string)
						}
						for k, v := range kvs {
							tablesData[tableName][k] = v
						}
					}
					delete(activeTxChanges, txID)
				}
				if deletes, ok := activeTxDeletes[txID]; ok {
					for tableName, keys := range deletes {
						if _, ok := tablesData[tableName]; ok {
							for k := range keys {
								delete(tablesData[tableName], k)
							}
						}
					}
					delete(activeTxDeletes, txID)
				}
				if drops, ok := activeTxDroppedTables[txID]; ok {
					for tableName := range drops {
						delete(tablesData, tableName)
					}
					delete(activeTxDroppedTables, txID)
				}
			}
		case "ROLLBACK_TX":
			if len(parts) == 2 { // ROLLBACK_TX <txID>
				txID := parts[1]
				// Discard buffered changes for this transaction
				delete(activeTxChanges, txID)
				delete(activeTxDeletes, txID)
				delete(activeTxDroppedTables, txID)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	// Convert the map[string]map[string]string to map[string][][2]string
	result := make(map[string][][2]string)
	for tableName, kvs := range tablesData {
		for k, v := range kvs {
			result[tableName] = append(result[tableName], [2]string{k, v})
		}
	}
	return result, nil
}
