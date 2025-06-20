package db

import (
	"os"
	"strings"
	"testing"
)

// setupTestEngine creates a new Engine instance for testing and ensures cleanup.
func setupTestEngine(t *testing.T) *Engine {
	t.Helper() // Marks the calling function as a test helper

	logPath := "test_wal.log"
	_ = os.Remove(logPath) // Clean up any old log file before starting the test

	engine := NewEngine(logPath)

	t.Cleanup(func() {
		// This function is called after the test (or sub-test) finishes
		os.Remove(logPath) // Remove the log file created during the test
	})
	return engine
}

func TestEngineInsertAndSelectAll(t *testing.T) {
	e := setupTestEngine(t)

	// Test basic insert into a new table
	insertCmd := `INSERT (a, 1), (b, 2), (c, 3) INTO mytable`
	resp := e.Execute(insertCmd)
	if resp != "OK" {
		t.Fatalf("Expected OK for insert, got %q", resp)
	}

	// Test selecting all from the table
	selectAllCmd := `SELECT * FROM mytable`
	resp = e.Execute(selectAllCmd)
	expectedLines := []string{"a: 1", "b: 2", "c: 3"}

	for _, line := range expectedLines {
		if !strings.Contains(resp, line) {
			t.Errorf("Expected result to contain %q, got:\n%s", line, resp)
		}
	}
	// Verify no unexpected lines
	if len(strings.Split(strings.TrimSpace(resp), "\n")) != len(expectedLines) {
		t.Errorf("Expected %d lines, got %d:\n%s", len(expectedLines), len(strings.Split(strings.TrimSpace(resp), "\n")), resp)
	}
}

func TestEngineSelectSpecificKeys(t *testing.T) {
	e := setupTestEngine(t)

	// Insert data into a test table
	insertCmd := `INSERT (key1, value1), (key2, value2), (key3, value3) INTO another_table`
	resp := e.Execute(insertCmd)
	if resp != "OK" {
		t.Fatalf("Expected OK for insert, got %q", resp)
	}

	// Test selecting a single key
	selectOneCmd := `SELECT key1 FROM another_table`
	resp = e.Execute(selectOneCmd)
	expectedOne := "key1: value1"
	if strings.TrimSpace(resp) != expectedOne {
		t.Fatalf("Expected '%s', got '%s'", expectedOne, resp)
	}

	// Test selecting multiple keys
	selectMultipleCmd := `SELECT key1, key3 FROM another_table`
	resp = e.Execute(selectMultipleCmd)
	expectedMultiple := []string{"key1: value1", "key3: value3"}
	// Order might not be guaranteed by map iteration, so check for containment
	for _, line := range expectedMultiple {
		if !strings.Contains(resp, line) {
			t.Errorf("Expected result to contain %q, got:\n%s", line, resp)
		}
	}
	if strings.Contains(resp, "key2:") {
		t.Errorf("Expected result NOT to contain key2, got:\n%s", resp)
	}
	if len(strings.Split(strings.TrimSpace(resp), "\n")) != len(expectedMultiple) {
		t.Errorf("Expected %d lines, got %d:\n%s", len(expectedMultiple), len(strings.Split(strings.TrimSpace(resp), "\n")), resp)
	}

	// Test selecting a non-existent key
	selectNonExistentCmd := `SELECT nonExistentKey FROM another_table`
	resp = e.Execute(selectNonExistentCmd)
	if resp != "No results" {
		t.Fatalf("Expected 'No results' for non-existent key, got %q", resp)
	}

	// Test selecting a mix of existing and non-existing keys
	selectMixedCmd := `SELECT key1, nonExistentKey FROM another_table`
	resp = e.Execute(selectMixedCmd)
	expectedMixed := "key1: value1" // Only existing key should be returned
	if strings.TrimSpace(resp) != expectedMixed {
		t.Fatalf("Expected '%s', got '%s'", expectedMixed, resp)
	}
}

func TestEngineDelete(t *testing.T) {
	e := setupTestEngine(t)

	// Insert data for deletion tests
	e.Execute(`INSERT (z, 99), (y, 88), (x, 77) INTO deletetable`)

	// Test deleting a single existing key
	delCmd := `DELETE z FROM deletetable`
	resp := e.Execute(delCmd)
	if resp != "Deleted 1 key(s) from table 'deletetable'" {
		t.Fatalf("Expected 'Deleted 1 key(s) from table 'deletetable'', got %q", resp)
	}

	// Verify 'z' is gone
	get := e.Execute(`SELECT * FROM deletetable`)
	if strings.Contains(get, "z: 99") {
		t.Fatalf("Expected 'z: 99' to be gone after delete, got %q", get)
	}

	// Test deleting multiple existing keys
	delMultipleCmd := `DELETE y, x FROM deletetable`
	resp = e.Execute(delMultipleCmd)
	if resp != "Deleted 2 key(s) from table 'deletetable'" {
		t.Fatalf("Expected 'Deleted 2 key(s) from table 'deletetable'', got %q", resp)
	}

	// Verify 'y' and 'x' are gone
	get = e.Execute(`SELECT * FROM deletetable`)
	if strings.Contains(get, "y: 88") || strings.Contains(get, "x: 77") {
		t.Fatalf("Expected 'y: 88' and 'x: 77' to be gone, got %q", get)
	}
	if strings.TrimSpace(get) != "No results" && strings.TrimSpace(get) != "" { // Table should be empty now
		t.Fatalf("Expected 'No results' or empty string, got %q", get)
	}

	// Test deleting a non-existent key
	delNonExistentCmd := `DELETE notfound FROM deletetable`
	resp = e.Execute(delNonExistentCmd)
	if resp != "No key(s) found to delete in table 'deletetable'" {
		t.Fatalf("Expected 'No key(s) found to delete...', got %q", resp)
	}

	// Test deleting from a non-existent table
	delFromNonExistentTable := `DELETE somekey FROM non_existent_table`
	resp = e.Execute(delFromNonExistentTable)
	if resp != "Table 'non_existent_table' not found" {
		t.Fatalf("Expected 'Table not found' error, got %q", resp)
	}
}

func TestEngineMultipleTables(t *testing.T) {
	e := setupTestEngine(t)

	// Insert into table A
	e.Execute(`INSERT (k1, v1_a), (k2, v2_a) INTO tableA`)
	// Insert into table B
	e.Execute(`INSERT (k1, v1_b), (k3, v3_b) INTO tableB`)

	// Select from table A
	respA := e.Execute(`SELECT * FROM tableA`)
	if !strings.Contains(respA, "k1: v1_a") || !strings.Contains(respA, "k2: v2_a") || strings.Contains(respA, "k1: v1_b") {
		t.Errorf("Mismatch in tableA. Got:\n%s", respA)
	}

	// Select from table B
	respB := e.Execute(`SELECT * FROM tableB`)
	if !strings.Contains(respB, "k1: v1_b") || !strings.Contains(respB, "k3: v3_b") || strings.Contains(respB, "k1: v1_a") {
		t.Errorf("Mismatch in tableB. Got:\n%s", respB)
	}

	// Delete from table A, ensure table B is unaffected
	e.Execute(`DELETE k1 FROM tableA`)
	respA = e.Execute(`SELECT * FROM tableA`)
	if strings.Contains(respA, "k1: v1_a") {
		t.Errorf("k1: v1_a should be deleted from tableA, got:\n%s", respA)
	}
	respB = e.Execute(`SELECT * FROM tableB`)
	if !strings.Contains(respB, "k1: v1_b") {
		t.Errorf("k1: v1_b should still be in tableB, got:\n%s", respB)
	}
}

func TestEngineDropTable(t *testing.T) {
	e := setupTestEngine(t)

	e.Execute(`INSERT (u1, Alice), (u2, Bob) INTO users_to_drop`)
	e.Execute(`INSERT (p1, pen), (p2, paper) INTO products_to_keep`) // Another table to ensure it's not affected

	// Drop the 'users_to_drop' table
	resp := e.Execute(`DROP users_to_drop`)
	if resp != "Table 'users_to_drop' dropped" {
		t.Fatalf("Expected 'Table 'users_to_drop' dropped', got %q", resp)
	}

	// Verify the dropped table no longer exists
	resp = e.Execute(`SELECT * FROM users_to_drop`)
	if resp != "Table 'users_to_drop' not found" {
		t.Fatalf("Expected 'Table not found' after drop, got %q", resp)
	}

	// Verify other tables are unaffected
	resp = e.Execute(`SELECT * FROM products_to_keep`)
	if !strings.Contains(resp, "p1: pen") || !strings.Contains(resp, "p2: paper") {
		t.Fatalf("Expected products_to_keep to be intact, got:\n%s", resp)
	}

	// Test dropping a non-existent table
	resp = e.Execute(`DROP non_existent_table`)
	if resp != "Table 'non_existent_table' not found" {
		t.Fatalf("Expected 'Table not found' when dropping non-existent table, got %q", resp)
	}
}

func TestEngineInvalidSyntax(t *testing.T) {
	e := setupTestEngine(t)

	// Test invalid INSERT syntax
	resp := e.Execute(`INSERT`)
	if !strings.HasPrefix(resp, "Parse error:") {
		t.Fatalf("Expected parse error, got %q", resp)
	}
	resp = e.Execute(`INSERT INTO mytable`) // Missing VALUES
	if !strings.HasPrefix(resp, "Parse error:") {
		t.Fatalf("Expected parse error, got %q", resp)
	}
	resp = e.Execute(`INSERT (k,v) FROM mytable`) // Missing INTO
	if !strings.HasPrefix(resp, "Parse error:") {
		t.Fatalf("Expected parse error, got %q", resp)
	}

	// Test invalid SELECT syntax
	resp = e.Execute(`SELECT key1 FROM`) // Missing table name
	if !strings.HasPrefix(resp, "Parse error:") {
		t.Fatalf("Expected parse error for invalid SELECT, got %q", resp)
	}
	resp = e.Execute(`SELECT FROM mytable`) // Missing keys or *
	if !strings.HasPrefix(resp, "Parse error:") {
		t.Fatalf("Expected parse error for invalid SELECT, got %q", resp)
	}

	// Test invalid DELETE syntax
	resp = e.Execute(`DELETE FROM mytable`) // Missing keys
	if !strings.HasPrefix(resp, "Parse error:") {
		t.Fatalf("Expected parse error for invalid DELETE, got %q", resp)
	}
	resp = e.Execute(`DELETE k1 mytable`) // Missing FROM
	if !strings.HasPrefix(resp, "Parse error:") {
		t.Fatalf("Expected parse error for invalid DELETE, got %q", resp)
	}

	// Test invalid DROP syntax
	resp = e.Execute(`DROP`) // Missing TABLE
	if !strings.HasPrefix(resp, "Parse error:") {
		t.Fatalf("Expected parse error for invalid DROP, got %q", resp)
	}
}

func TestEngineUnsupportedStatement(t *testing.T) {
	e := setupTestEngine(t)

	resp := e.Execute(`UPDATE table SET key = val`) // not supported
	if !strings.HasPrefix(resp, "Parse error:") {
		t.Fatalf("Expected parse error, got %q", resp)
	}
}
