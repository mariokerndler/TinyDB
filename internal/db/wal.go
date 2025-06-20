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

// Append logs a SET operation for a specific table, key, and value.
func (w *WAL) Append(tableName, key, value string) {
	fmt.Fprintf(w.file, "SET %s %s %s\n", tableName, key, value)
}

// Delete logs a DELETE operation for a specific table and key.
func (w *WAL) Delete(tableName, key string) {
	fmt.Fprintf(w.file, "DELETE %s %s\n", tableName, key)
}

// DropTable logs a DROP TABLE operation for a specific table.
func (w *WAL) DropTable(tableName string) {
	fmt.Fprintf(w.file, "DROP TABLE %s\n", tableName)
}

// Replay reads the WAL and reconstructs the state of all tables.
// It returns a map where keys are table names and values are slices of [key, value] pairs.
func (w *WAL) Replay() (map[string][][2]string, error) {
	f, err := os.Open(w.path)
	if err != nil {
		// If the log file doesn't exist, return an empty map and no error.
		if os.IsNotExist(err) {
			return make(map[string][][2]string), nil
		}
		return nil, err
	}
	defer f.Close()

	// Use a map to store current state of each table's keys
	// This helps handle SET/DELETE operations correctly during replay.
	tablesData := make(map[string]map[string]string)

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
			if len(parts) == 4 { // SET <table_name> <key> <value>
				tableName := parts[1]
				key := parts[2]
				value := parts[3]
				if _, ok := tablesData[tableName]; !ok {
					tablesData[tableName] = make(map[string]string)
				}
				tablesData[tableName][key] = value
			}
		case "DELETE":
			if len(parts) == 3 { // DELETE <table_name> <key>
				tableName := parts[1]
				key := parts[2]
				if _, ok := tablesData[tableName]; ok {
					delete(tablesData[tableName], key)
				}
			}
		case "DROP":
			if len(parts) == 3 && strings.ToUpper(parts[1]) == "TABLE" { // DROP TABLE <table_name>
				tableName := parts[2]
				delete(tablesData, tableName) // Remove the entire table from memory
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	// Convert the map[string]map[string]string to map[string][][2]string
	// for the final return value, suitable for initializing BPlusTrees.
	result := make(map[string][][2]string)
	for tableName, tableMap := range tablesData {
		var entries [][2]string
		for key, value := range tableMap {
			entries = append(entries, [2]string{key, value})
		}
		result[tableName] = entries
	}

	return result, nil
}
