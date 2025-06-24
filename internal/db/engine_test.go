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
	// Updated expected response for INSERT
	if resp != "Inserted 3 key(s) into table 'mytable'" {
		t.Fatalf("Expected 'Inserted 3 key(s) into table 'mytable'' for insert, got %q", resp)
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
	// Updated expected response for INSERT
	if resp != "Inserted 3 key(s) into table 'another_table'" {
		t.Fatalf("Expected 'Inserted 3 key(s) into table 'another_table'' for insert, got %q", resp)
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

	// Test invalid UPDATE syntax
	resp = e.Execute(`UPDATE mytable SET`) // Missing key-value pairs
	if !strings.HasPrefix(resp, "Parse error:") {
		t.Fatalf("Expected parse error for invalid UPDATE, got %q", resp)
	}
	resp = e.Execute(`UPDATE mytable (k,v)`) // Missing SET keyword
	if !strings.HasPrefix(resp, "Parse error:") {
		t.Fatalf("Expected parse error for invalid UPDATE, got %q", resp)
	}
}

func TestEngineUnsupportedStatement(t *testing.T) {
	e := setupTestEngine(t)

	// This test is no longer relevant as UPDATE is now supported and tested separately
	// It's kept here just to confirm any other unknown commands still result in a parse error.
	resp := e.Execute(`UNKNOWN COMMAND`)
	if !strings.HasPrefix(resp, "Parse error:") {
		t.Fatalf("Expected parse error for unknown command, got %q", resp)
	}
}

func TestEngineUpdateCommand(t *testing.T) {
	e := setupTestEngine(t)

	// Insert initial data
	e.Execute(`INSERT (k1, v1), (k2, v2) INTO update_table`)

	// Test successful update
	resp := e.Execute(`UPDATE update_table SET (k1, v1_updated), (k3, v3_new_not_exist)`) // k3 does not exist, should not update
	if resp != "Updated 1 key(s) in table 'update_table'" {
		t.Fatalf("Expected 'Updated 1 key(s) in table 'update_table'', got %q", resp)
	}

	// Verify updated value
	resp = e.Execute(`SELECT k1 FROM update_table`)
	if strings.TrimSpace(resp) != "k1: v1_updated" {
		t.Fatalf("Expected k1: v1_updated, got %q", resp)
	}

	// Verify non-updated key (k2 should remain original)
	resp = e.Execute(`SELECT k2 FROM update_table`)
	if strings.TrimSpace(resp) != "k2: v2" {
		t.Fatalf("Expected k2: v2, got %q", resp)
	}

	// Verify k3 was not inserted by UPDATE
	resp = e.Execute(`SELECT k3 FROM update_table`)
	if resp != "No results" {
		t.Fatalf("Expected 'No results' for k3, got %q", resp)
	}

	// Test updating a non-existent table
	resp = e.Execute(`UPDATE non_existent_table SET (k1, v1)`)
	if resp != "Table 'non_existent_table' not found" {
		t.Fatalf("Expected 'Table not found' error, got %q", resp)
	}

	// Test updating keys that don't exist in an existing table
	e.Execute(`INSERT (key_a, val_a) INTO existing_table`)
	resp = e.Execute(`UPDATE existing_table SET (key_b, val_b)`)
	if resp != "No keys found to update" {
		t.Fatalf("Expected 'No keys found to update', got %q", resp)
	}
}

func TestEngineInsertOnlyNewKeys(t *testing.T) {
	e := setupTestEngine(t)

	// Insert initial data
	e.Execute(`INSERT (key_x, val_x), (key_y, val_y) INTO insert_only_table`)

	// Attempt to insert an existing key and a new key
	resp := e.Execute(`INSERT (key_x, val_x_new), (key_z, val_z) INTO insert_only_table`)
	if resp != "Inserted 1 key(s) into table 'insert_only_table'" { // Only key_z should be inserted
		t.Fatalf("Expected 'Inserted 1 key(s) into table 'insert_only_table'', got %q", resp)
	}

	// Verify key_x value is still the original one (not updated by INSERT)
	resp = e.Execute(`SELECT key_x FROM insert_only_table`)
	if strings.TrimSpace(resp) != "key_x: val_x" {
		t.Fatalf("Expected key_x: val_x, got %q", resp)
	}

	// Verify new key_z was inserted
	resp = e.Execute(`SELECT key_z FROM insert_only_table`)
	if strings.TrimSpace(resp) != "key_z: val_z" {
		t.Fatalf("Expected key_z: val_z, got %q", resp)
	}

	// Attempt to insert only existing keys
	resp = e.Execute(`INSERT (key_x, val_x_again), (key_y, val_y_again) INTO insert_only_table`)
	if resp != "No new keys inserted (they might already exist)" {
		t.Fatalf("Expected 'No new keys inserted (they might already exist)', got %q", resp)
	}
}
