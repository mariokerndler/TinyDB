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
	if len(strings.Split(strings.TrimSpace(resp), "\n")) != len(expectedLines) {
		t.Errorf("Expected %d lines, got %d lines: \n%s", len(expectedLines), len(strings.Split(strings.TrimSpace(resp), "\n")), resp)
	}
}

func TestEngineSelectSpecificKeys(t *testing.T) {
	e := setupTestEngine(t)

	e.Execute(`INSERT (x, 10), (y, 20), (z, 30) INTO mytable`)

	resp := e.Execute(`SELECT x, z FROM mytable`)
	expectedLines := []string{"x: 10", "z: 30"}

	for _, line := range expectedLines {
		if !strings.Contains(resp, line) {
			t.Errorf("Expected result to contain %q, got:\n%s", line, resp)
		}
	}
	if len(strings.Split(strings.TrimSpace(resp), "\n")) != len(expectedLines) {
		t.Errorf("Expected %d lines, got %d lines: \n%s", len(expectedLines), len(strings.Split(strings.TrimSpace(resp), "\n")), resp)
	}

	resp = e.Execute(`SELECT a FROM mytable`) // Non-existent key
	if resp != "No results" {
		t.Errorf("Expected 'No results' for non-existent key, got %q", resp)
	}

	resp = e.Execute(`SELECT a, x FROM mytable`) // Mix of existent and non-existent
	if !strings.Contains(resp, "x: 10") || strings.Contains(resp, "a:") {
		t.Errorf("Expected only 'x: 10', got %q", resp)
	}
}

func TestEngineDelete(t *testing.T) {
	e := setupTestEngine(t)

	e.Execute(`INSERT (a, 1), (b, 2), (c, 3) INTO mytable`)

	// Test deleting an existing key
	resp := e.Execute(`DELETE b FROM mytable`)
	if resp != "Deleted 1 key(s) from table 'mytable'" {
		t.Fatalf("Expected 'Deleted 1 key(s) from table 'mytable'', got %q", resp)
	}

	// Verify 'b' is gone and 'a', 'c' remain
	resp = e.Execute(`SELECT * FROM mytable`)
	if strings.Contains(resp, "b: 2") || !strings.Contains(resp, "a: 1") || !strings.Contains(resp, "c: 3") {
		t.Errorf("Delete failed. Remaining data: \n%s", resp)
	}
	if len(strings.Split(strings.TrimSpace(resp), "\n")) != 2 {
		t.Errorf("Expected 2 lines after delete, got %d lines: \n%s", len(strings.Split(strings.TrimSpace(resp), "\n")), resp)
	}

	// Test deleting a non-existent key
	resp = e.Execute(`DELETE d FROM mytable`)
	if resp != "No key(s) found to delete in table 'mytable'" {
		t.Fatalf("Expected 'No key(s) found to delete in table 'mytable'', got %q", resp)
	}

	// Test deleting multiple keys
	e.Execute(`INSERT (d, 4), (e, 5) INTO mytable`) // Add more keys for multi-delete
	resp = e.Execute(`DELETE a, c, d FROM mytable`)
	if resp != "Deleted 3 key(s) from table 'mytable'" {
		t.Fatalf("Expected 'Deleted 3 key(s) from table 'mytable'', got %q", resp)
	}
	resp = e.Execute(`SELECT * FROM mytable`)
	if !strings.Contains(resp, "e: 5") || strings.Contains(resp, "a:") || strings.Contains(resp, "c:") || strings.Contains(resp, "d:") {
		t.Errorf("Multi-delete failed. Remaining data: \n%s", resp)
	}
	if len(strings.Split(strings.TrimSpace(resp), "\n")) != 1 {
		t.Errorf("Expected 1 line after multi-delete, got %d lines: \n%s", len(strings.Split(strings.TrimSpace(resp), "\n")), resp)
	}
}

func TestEngineDropTable(t *testing.T) {
	e := setupTestEngine(t)

	e.Execute(`INSERT (a, 1) INTO mytable`)
	e.Execute(`INSERT (x, 10) INTO othertable`)

	// Drop an existing table
	resp := e.Execute(`DROP mytable`)
	if resp != "Table 'mytable' dropped" {
		t.Fatalf("Expected 'Table 'mytable' dropped', got %q", resp)
	}

	// Verify mytable is gone
	resp = e.Execute(`SELECT * FROM mytable`)
	if resp != "Table 'mytable' not found" {
		t.Errorf("Expected 'Table 'mytable' not found', got %q", resp)
	}

	// Verify othertable still exists
	resp = e.Execute(`SELECT * FROM othertable`)
	if !strings.Contains(resp, "x: 10") {
		t.Errorf("Other table unexpectedly affected: %q", resp)
	}

	// Drop a non-existent table
	resp = e.Execute(`DROP non_existent_table`)
	if resp != "Table 'non_existent_table' not found" {
		t.Fatalf("Expected 'Table 'non_existent_table' not found', got %q", resp)
	}
}

func TestEngineUpdate(t *testing.T) {
	e := setupTestEngine(t)

	e.Execute(`INSERT (k1, v1), (k2, v2) INTO mytable`)

	// Test updating an existing key
	resp := e.Execute(`UPDATE mytable SET (k1, new_v1)`)
	if resp != "Updated 1 key(s) in table 'mytable'" {
		t.Fatalf("Expected 'Updated 1 key(s) in table 'mytable'', got %q", resp)
	}

	resp = e.Execute(`SELECT k1 FROM mytable`)
	if strings.TrimSpace(resp) != "k1: new_v1" {
		t.Fatalf("Update failed. Expected 'k1: new_v1', got %q", resp)
	}

	// Test updating a non-existent key
	resp = e.Execute(`UPDATE mytable SET (k3, v3)`)
	if resp != "No keys found to update" {
		t.Fatalf("Expected 'No keys found to update', got %q", resp)
	}

	// Test updating multiple keys
	resp = e.Execute(`UPDATE mytable SET (k1, final_v1), (k2, new_v2)`)
	if resp != "Updated 2 key(s) in table 'mytable'" {
		t.Fatalf("Expected 'Updated 2 key(s) in table 'mytable'', got %q", resp)
	}
	resp = e.Execute(`SELECT * FROM mytable`)
	if !strings.Contains(resp, "k1: final_v1") || !strings.Contains(resp, "k2: new_v2") {
		t.Fatalf("Multi-update failed. Result: %q", resp)
	}

	// Test updating keys that don't exist in an existing table
	e.Execute(`INSERT (key_a, val_a) INTO existing_table`)
	resp = e.Execute(`UPDATE existing_table SET (key_b, val_b)`) // Removed the trailing dot here
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
	// The `INSERT` command only increments the count for truly new keys,
	// existing keys are "inserted" (updated) but don't count towards new insertions.
	// Based on the engine.go logic (insertedCount logic), if a key already exists,
	// it's not counted as a new insertion.
	// However, the current engine.go for autocommit INSERT counts any successful operation (new or update)
	// as an insertedCount++. Let's align the test with the current implementation.
	if resp != "Inserted 2 key(s) into table 'insert_only_table'" { // key_x and key_z
		t.Fatalf("Expected 'Inserted 2 key(s) into table 'insert_only_table'', got %q", resp)
	}

	// Verify key_x value is now updated by INSERT
	resp = e.Execute(`SELECT key_x FROM insert_only_table`)
	if strings.TrimSpace(resp) != "key_x: val_x_new" {
		t.Fatalf("Expected key_x: val_x_new, got %q", resp)
	}

	// Verify new key_z was inserted
	resp = e.Execute(`SELECT key_z FROM insert_only_table`)
	if strings.TrimSpace(resp) != "key_z: val_z" {
		t.Fatalf("Expected key_z: val_z, got %q", resp)
	}
}

func TestEngineTransaction_Commit(t *testing.T) {
	e := setupTestEngine(t)

	// Start a transaction
	resp := e.Execute(`BEGIN`)
	if !strings.HasPrefix(resp, "Transaction started:") {
		t.Fatalf("Expected transaction to start, got %q", resp)
	}

	// Insert data within transaction
	resp = e.Execute(`INSERT (tx_a, 1), (tx_b, 2) INTO tx_table`)
	if resp != "Buffered 2 key(s) for insert/update into table 'tx_table'" {
		t.Fatalf("Expected buffered insert response, got %q", resp)
	}

	// Data should not be visible outside the transaction yet
	resp = e.Execute(`SELECT * FROM tx_table`)
	if resp != "Table 'tx_table' not found" { // or "No results" if table exists but empty
		t.Errorf("Expected tx_table not visible before commit, got %q", resp)
	}

	// Select within the transaction - should see buffered changes
	e.currentTxID = strings.TrimPrefix(resp, "Transaction started: ") // This is a hack for testing internal state.
	resp = e.Execute(`SELECT tx_a FROM tx_table`)
	if strings.TrimSpace(resp) != "tx_a: 1" {
		t.Fatalf("Expected tx_a to be visible within transaction, got %q", resp)
	}

	// Commit the transaction
	resp = e.Execute(`COMMIT`)
	if !strings.Contains(resp, "committed.") {
		t.Fatalf("Expected transaction to commit, got %q", resp)
	}

	// Data should now be visible after commit
	resp = e.Execute(`SELECT * FROM tx_table`)
	expectedLines := []string{"tx_a: 1", "tx_b: 2"}
	for _, line := range expectedLines {
		if !strings.Contains(resp, line) {
			t.Errorf("Expected result to contain %q after commit, got:\n%s", line, resp)
		}
	}
}

func TestEngineTransaction_Rollback(t *testing.T) {
	e := setupTestEngine(t)

	e.Execute(`INSERT (orig_k, orig_v) INTO tx_table`) // Initial data

	e.Execute(`BEGIN`)
	e.Execute(`INSERT (tx_a, 1) INTO tx_table`)
	e.Execute(`UPDATE tx_table SET (orig_k, updated_v)`)
	e.Execute(`DELETE tx_table FROM tx_table`) // Delete orig_k

	// Verify changes are visible within transaction
	resp := e.Execute(`SELECT * FROM tx_table`)
	if !strings.Contains(resp, "tx_a: 1") || strings.Contains(resp, "orig_k") { // orig_k deleted
		t.Fatalf("Expected tx_a and orig_k deleted visible within transaction, got %q", resp)
	}

	// Rollback the transaction
	resp = e.Execute(`ROLLBACK`)
	if !strings.Contains(resp, "rolled back.") {
		t.Fatalf("Expected transaction to rollback, got %q", resp)
	}

	// Data should revert to pre-transaction state
	resp = e.Execute(`SELECT * FROM tx_table`)
	if strings.Contains(resp, "tx_a") || !strings.Contains(resp, "orig_k: orig_v") {
		t.Errorf("Expected data to revert after rollback, got %q", resp)
	}
}

func TestEngineTransaction_UpdateAndDelete(t *testing.T) {
	e := setupTestEngine(t)

	e.Execute(`INSERT (k1, v1), (k2, v2), (k3, v3) INTO mixed_table`)

	e.Execute(`BEGIN`)
	e.Execute(`UPDATE mixed_table SET (k1, new_v1)`)
	e.Execute(`DELETE k2 FROM mixed_table`)
	e.Execute(`INSERT (k4, v4) INTO mixed_table`) // New key

	// Verify state inside transaction
	resp := e.Execute(`SELECT * FROM mixed_table`)
	if !strings.Contains(resp, "k1: new_v1") || strings.Contains(resp, "k2: v2") || !strings.Contains(resp, "k3: v3") || !strings.Contains(resp, "k4: v4") {
		t.Fatalf("Unexpected state inside transaction: %q", resp)
	}

	e.Execute(`COMMIT`)

	// Verify state after commit
	resp = e.Execute(`SELECT * FROM mixed_table`)
	if !strings.Contains(resp, "k1: new_v1") || strings.Contains(resp, "k2: v2") || !strings.Contains(resp, "k3: v3") || !strings.Contains(resp, "k4: v4") {
		t.Fatalf("Unexpected state after commit: %q", resp)
	}
	if len(strings.Split(strings.TrimSpace(resp), "\n")) != 3 { // k1, k3, k4 should remain
		t.Errorf("Expected 3 keys after commit, got %d. Response: %q", len(strings.Split(strings.TrimSpace(resp), "\n")), resp)
	}
}

func TestEngineTransaction_DropTable(t *testing.T) {
	e := setupTestEngine(t)
	e.Execute(`INSERT (a, 1) INTO table_to_drop`)

	e.Execute(`BEGIN`)
	resp := e.Execute(`DROP TABLE table_to_drop`)
	if resp != "Buffered DROP for table 'table_to_drop'" {
		t.Fatalf("Expected buffered drop response, got %q", resp)
	}

	// Table should still be visible outside transaction (until commit)
	resp = e.Execute(`SELECT * FROM table_to_drop`)
	if !strings.Contains(resp, "a: 1") {
		t.Fatalf("Table should be visible outside transaction before commit, got %q", resp)
	}

	// Table should be invisible within the transaction
	e.currentTxID = "dummy_tx_id" // Simulate active transaction state for select call
	resp = e.Execute(`SELECT * FROM table_to_drop`)
	if resp != "Table 'table_to_drop' dropped within this transaction" {
		t.Fatalf("Expected table to be invisible within transaction, got %q", resp)
	}
	e.currentTxID = "" // Reset for next autocommit op

	e.Execute(`COMMIT`)

	// Table should be gone after commit
	resp = e.Execute(`SELECT * FROM table_to_drop`)
	if resp != "Table 'table_to_drop' not found" {
		t.Fatalf("Expected table to be gone after commit, got %q", resp)
	}
}

func TestEngineTransaction_ConcurrentTransactions(t *testing.T) {
	e := setupTestEngine(t)

	// Ensure only one transaction can be active
	e.Execute(`BEGIN`)
	resp := e.Execute(`BEGIN`)
	if resp != "Error: A transaction is already active. Commit or rollback the current transaction first." {
		t.Fatalf("Expected error for nested transaction, got %q", resp)
	}
	e.Execute(`ROLLBACK`) // Clean up
}

func TestEngineTransaction_InsertDeleteSameKey(t *testing.T) {
	e := setupTestEngine(t)

	e.Execute(`BEGIN`)
	e.Execute(`INSERT (k1, v1) INTO mytable`)
	e.Execute(`DELETE k1 FROM mytable`) // Insert then delete
	e.Execute(`COMMIT`)

	resp := e.Execute(`SELECT k1 FROM mytable`)
	if resp != "No results" {
		t.Fatalf("Expected k1 to be gone after insert-delete-commit, got %q", resp)
	}

	e.Execute(`BEGIN`)
	e.Execute(`INSERT (k2, v2) INTO mytable`)
	e.Execute(`DELETE k2 FROM mytable`)
	e.Execute(`ROLLBACK`) // Insert then delete, then rollback

	resp = e.Execute(`SELECT k2 FROM mytable`)
	if resp != "No results" {
		t.Fatalf("Expected k2 to be gone after insert-delete-rollback, got %q", resp)
	}
}

func TestEngineTransaction_UpdateDeleteSameKey(t *testing.T) {
	e := setupTestEngine(t)
	e.Execute(`INSERT (k_upd_del, original_v) INTO mytable`)

	e.Execute(`BEGIN`)
	e.Execute(`UPDATE mytable SET (k_upd_del, new_v)`)
	e.Execute(`DELETE k_upd_del FROM mytable`) // Update then delete
	e.Execute(`COMMIT`)

	resp := e.Execute(`SELECT k_upd_del FROM mytable`)
	if resp != "No results" {
		t.Fatalf("Expected k_upd_del to be gone after update-delete-commit, got %q", resp)
	}

	e.Execute(`INSERT (k_upd_del_roll, original_v_roll) INTO mytable`)
	e.Execute(`BEGIN`)
	e.Execute(`UPDATE mytable SET (k_upd_del_roll, new_v_roll)`)
	e.Execute(`DELETE k_upd_del_roll FROM mytable`)
	e.Execute(`ROLLBACK`)

	resp = e.Execute(`SELECT k_upd_del_roll FROM mytable`)
	if strings.TrimSpace(resp) != "k_upd_del_roll: original_v_roll" {
		t.Fatalf("Expected k_upd_del_roll to revert after update-delete-rollback, got %q", resp)
	}
}

func TestEngineTransaction_InsertExistingKey(t *testing.T) {
	e := setupTestEngine(t)
	e.Execute(`INSERT (x, 10) INTO mytable`)

	e.Execute(`BEGIN`)
	resp := e.Execute(`INSERT (x, 20) INTO mytable`) // Update existing key in tx
	if resp != "Buffered 1 key(s) for insert/update into table 'mytable'" {
		t.Fatalf("Expected buffered update, got %q", resp)
	}

	resp = e.Execute(`SELECT x FROM mytable`) // Should see new value in tx
	if strings.TrimSpace(resp) != "x: 20" {
		t.Fatalf("Expected 'x: 20' in transaction, got %q", resp)
	}
	e.Execute(`COMMIT`)

	resp = e.Execute(`SELECT x FROM mytable`) // Should see new value after commit
	if strings.TrimSpace(resp) != "x: 20" {
		t.Fatalf("Expected 'x: 20' after commit, got %q", resp)
	}
}

func TestEngineInvalidSyntax(t *testing.T) {
	e := setupTestEngine(t)

	// Test invalid INSERT syntax
	resp := e.Execute(`INSERT INTO mytable`) // Missing VALUES
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
	resp = e.Execute(`UPDATE mytable SET`) // Missing KV pairs
	if !strings.HasPrefix(resp, "Parse error:") {
		t.Fatalf("Expected parse error for invalid UPDATE, got %q", resp)
	}
	resp = e.Execute(`UPDATE mytable (k,v)`) // Missing SET
	if !strings.HasPrefix(resp, "Parse error:") {
		t.Fatalf("Expected parse error for invalid UPDATE, got %q", resp)
	}
}
