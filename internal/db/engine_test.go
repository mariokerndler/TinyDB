package db

import (
	"fmt" // Import fmt for Sprintf
	"os"
	"strings"
	"testing"
)

// setupTestEngine creates a new Engine instance for testing and ensures cleanup.
func setupTestEngine(t *testing.T) *Engine {
	t.Helper()

	logPath := "test_wal.log"
	_ = os.Remove(logPath)

	engine := NewEngine(logPath)

	t.Cleanup(func() {
		_ = os.Remove(logPath)
	})
	return engine
}

func TestEngineInsertAndSelectAll(t *testing.T) {
	e := setupTestEngine(t)

	insertCmd := `INSERT (a, 1), (b, 2), (c, 3) INTO mytable`
	resp := e.Execute(insertCmd)
	if resp != "Inserted 3 key(s) into table 'mytable'" {
		t.Fatalf("Expected 'Inserted 3 key(s) into table 'mytable'' for insert, got %q", resp)
	}

	selectAllCmd := `SELECT * FROM mytable`
	resp = e.Execute(selectAllCmd)
	expectedLines := []string{"a: 1", "b: 2", "c: 3"}

	for _, line := range expectedLines {
		if !strings.Contains(resp, line) {
			t.Errorf("Expected result to contain %q, got:\n%s", line, resp)
		}
	}
	if len(strings.Split(strings.TrimSpace(resp), "\n")) != len(expectedLines) {
		t.Errorf("Expected %d lines, got %d", len(expectedLines), len(strings.Split(strings.TrimSpace(resp), "\n")))
	}
}

func TestEngineSelectSpecificKeys(t *testing.T) {
	e := setupTestEngine(t)

	e.Execute(`INSERT (x, 10), (y, 20), (z, 30) INTO specific_table`)

	resp := e.Execute(`SELECT x, z FROM specific_table`)
	expectedLines := []string{"x: 10", "z: 30"}
	for _, line := range expectedLines {
		if !strings.Contains(resp, line) {
			t.Errorf("Expected result to contain %q, got:\n%s", line, resp)
		}
	}
	if len(strings.Split(strings.TrimSpace(resp), "\n")) != len(expectedLines) {
		t.Errorf("Expected %d lines, got %d", len(expectedLines), len(strings.Split(strings.TrimSpace(resp), "\n")))
	}

	resp = e.Execute(`SELECT non_existent FROM specific_table`)
	if resp != "No results" {
		t.Errorf("Expected 'No results' for non-existent key, got %q", resp)
	}
}

func TestEngineDelete(t *testing.T) {
	e := setupTestEngine(t)
	e.Execute(`INSERT (a, 1), (b, 2), (c, 3) INTO delete_table`)

	resp := e.Execute(`DELETE a FROM delete_table`)
	if resp != "Deleted 1 key(s) from table 'delete_table'" {
		t.Fatalf("Expected 'Deleted 1 key(s) from table 'delete_table'', got %q", resp)
	}

	resp = e.Execute(`SELECT * FROM delete_table`)
	if strings.Contains(resp, "a: 1") {
		t.Errorf("Expected 'a: 1' to be deleted, but it's still present:\n%s", resp)
	}

	resp = e.Execute(`DELETE non_existent FROM delete_table`)
	if resp != "No key(s) found to delete in table 'delete_table'" {
		t.Errorf("Expected 'No key(s) found to delete...', got %q", resp)
	}
}

func TestEngineDropTable(t *testing.T) {
	e := setupTestEngine(t)
	e.Execute(`INSERT (k, v) INTO table_to_drop`)

	resp := e.Execute(`DROP table_to_drop`)
	if resp != "Table 'table_to_drop' dropped" {
		t.Fatalf("Expected 'Table 'table_to_drop' dropped', got %q", resp)
	}

	resp = e.Execute(`SELECT * FROM table_to_drop`)
	if resp != "Table 'table_to_drop' not found" {
		t.Errorf("Expected 'Table 'table_to_drop' not found' after drop, got %q", resp)
	}
}

func TestEngineUpdate(t *testing.T) {
	e := setupTestEngine(t)
	e.Execute(`INSERT (k1, v1), (k2, v2) INTO update_table`)

	resp := e.Execute(`UPDATE update_table SET (k1, new_v1)`)
	if resp != "Updated 1 key(s) in table 'update_table'" {
		t.Fatalf("Expected 'Updated 1 key(s)...', got %q", resp)
	}

	resp = e.Execute(`SELECT k1 FROM update_table`)
	if strings.TrimSpace(resp) != "k1: new_v1" {
		t.Fatalf("Expected k1: new_v1, got %q", resp)
	}

	resp = e.Execute(`UPDATE update_table SET (non_existent, new_val)`)
	if resp != "No keys found to update" {
		t.Fatalf("Expected 'No keys found to update', got %q", resp)
	}
}

func TestEngineTransactionIsolation(t *testing.T) {
	e := setupTestEngine(t)

	txResp := e.Execute(`BEGIN`)
	if !strings.HasPrefix(txResp, "Transaction started: tx_") {
		t.Fatalf("Expected transaction to start, got %q", txResp)
	}
	txID := strings.TrimPrefix(txResp, "Transaction started: ")

	e.Execute(`INSERT (tx_key1, tx_val1), (common_key, tx_common_val) INTO tx_table`)

	selectResp := e.Execute(`SELECT * FROM tx_table`)
	expectedInTx := []string{
		fmt.Sprintf("common_key: [tx_%s] tx_common_val", strings.TrimPrefix(txID, "tx_")),
		fmt.Sprintf("tx_key1: [tx_%s] tx_val1", strings.TrimPrefix(txID, "tx_")),
	}
	for _, line := range expectedInTx {
		if !strings.Contains(selectResp, line) {
			t.Errorf("Expected SELECT in transaction to contain %q, got:\n%s", line, selectResp)
		}
	}
	if len(strings.Split(strings.TrimSpace(selectResp), "\n")) != len(expectedInTx) {
		t.Errorf("Expected %d lines in transaction SELECT, got %d:\n%s", len(expectedInTx), len(strings.Split(strings.TrimSpace(selectResp), "\n")), selectResp)
	}

	e.Execute(`UPDATE tx_table SET (tx_key1, tx_val1_updated)`)
	e.Execute(`UPDATE tx_table SET (common_key, tx_common_val_updated)`)

	selectResp = e.Execute(`SELECT * FROM tx_table`)
	expectedInTxAfterUpdate := []string{
		fmt.Sprintf("common_key: [tx_%s] tx_common_val_updated", strings.TrimPrefix(txID, "tx_")),
		fmt.Sprintf("tx_key1: [tx_%s] tx_val1_updated", strings.TrimPrefix(txID, "tx_")),
	}
	for _, line := range expectedInTxAfterUpdate {
		if !strings.Contains(selectResp, line) {
			t.Errorf("Expected SELECT in transaction after update to contain %q, got:\n%s", line, selectResp)
		}
	}

	e.Execute(`DELETE tx_key1 FROM tx_table`)

	selectResp = e.Execute(`SELECT * FROM tx_table`)
	if strings.Contains(selectResp, "tx_key1") {
		t.Errorf("Expected tx_key1 to be deleted in transaction, but it's still present:\n%s", selectResp)
	}
	expectedInTxAfterDelete := []string{
		fmt.Sprintf("common_key: [tx_%s] tx_common_val_updated", strings.TrimPrefix(txID, "tx_")),
	}
	for _, line := range expectedInTxAfterDelete {
		if !strings.Contains(selectResp, line) {
			t.Errorf("Expected SELECT in transaction after delete to contain %q, got:\n%s", line, selectResp)
		}
	}
	if len(strings.Split(strings.TrimSpace(selectResp), "\n")) != len(expectedInTxAfterDelete) {
		t.Errorf("Expected %d lines in transaction SELECT after delete, got %d:\n%s", len(expectedInTxAfterDelete), len(strings.Split(strings.TrimSpace(selectResp), "\n")), selectResp)
	}

	commitResp := e.Execute(`COMMIT`)
	if commitResp != fmt.Sprintf("Transaction %s committed.", txID) {
		t.Fatalf("Expected commit success, got %q", commitResp)
	}

	selectResp = e.Execute(`SELECT * FROM tx_table`)
	expectedAfterCommit := []string{
		"common_key: tx_common_val_updated",
	}
	for _, line := range expectedAfterCommit {
		if !strings.Contains(selectResp, line) {
			t.Errorf("Expected SELECT after commit to contain %q, got:\n%s", line, selectResp)
		}
	}
	if strings.Contains(selectResp, "tx_key1") {
		t.Errorf("Expected tx_key1 to be permanently deleted after commit, but it's present:\n%s", selectResp)
	}
	if len(strings.Split(strings.TrimSpace(selectResp), "\n")) != len(expectedAfterCommit) {
		t.Errorf("Expected %d lines after commit, got %d:\n%s", len(expectedAfterCommit), len(strings.Split(strings.TrimSpace(selectResp), "\n")), selectResp)
	}

	txResp = e.Execute(`BEGIN`)
	if !strings.HasPrefix(txResp, "Transaction started: tx_") {
		t.Fatalf("Expected transaction to start, got %q", txResp)
	}
	txIDRollback := strings.TrimPrefix(txResp, "Transaction started: ")

	e.Execute(`INSERT (rollback_key, rollback_val) INTO tx_table`)
	e.Execute(`UPDATE tx_table SET (common_key, rolled_back_val)`)
	e.Execute(`DELETE common_key FROM tx_table`)

	selectResp = e.Execute(`SELECT * FROM tx_table`)
	if strings.Contains(selectResp, "common_key") {
		t.Errorf("Expected common_key to be deleted in rollback transaction, but it's present:\n%s", selectResp)
	}
	expectedInRollbackTx := []string{
		fmt.Sprintf("rollback_key: [tx_%s] rollback_val", strings.TrimPrefix(txIDRollback, "tx_")),
	}
	for _, line := range expectedInRollbackTx {
		if !strings.Contains(selectResp, line) {
			t.Errorf("Expected SELECT in rollback transaction to contain %q, got:\n%s", line, selectResp)
		}
	}
	if len(strings.Split(strings.TrimSpace(selectResp), "\n")) != len(expectedInRollbackTx) {
		t.Errorf("Expected %d lines in rollback transaction SELECT, got %d:\n%s", len(expectedInRollbackTx), len(strings.Split(strings.TrimSpace(selectResp), "\n")), selectResp)
	}

	rollbackResp := e.Execute(`ROLLBACK`)
	if rollbackResp != fmt.Sprintf("Transaction %s rolled back.", txIDRollback) {
		t.Fatalf("Expected rollback success, got %q", rollbackResp)
	}

	selectResp = e.Execute(`SELECT * FROM tx_table`)
	expectedAfterRollback := []string{
		"common_key: tx_common_val_updated",
	}
	for _, line := range expectedAfterRollback {
		if !strings.Contains(selectResp, line) {
			t.Errorf("Expected SELECT after rollback to contain %q, got:\n%s", line, selectResp)
		}
	}
	if strings.Contains(selectResp, "rollback_key") {
		t.Errorf("Expected rollback_key to be gone after rollback, but it's present:\n%s", selectResp)
	}
	if len(strings.Split(strings.TrimSpace(selectResp), "\n")) != len(expectedAfterRollback) {
		t.Errorf("Expected %d lines after rollback, got %d:\n%s", len(expectedAfterRollback), len(strings.Split(strings.TrimSpace(selectResp), "\n")), selectResp)
	}
}

func TestEngineMixedOperations(t *testing.T) {
	e := setupTestEngine(t)

	e.Execute(`INSERT (k1, v1), (k2, v2_orig) INTO mixed_table`)

	txResp := e.Execute(`BEGIN`)
	if !strings.HasPrefix(txResp, "Transaction started: tx_") {
		t.Fatalf("Expected transaction to start, got %q", txResp)
	}
	txID := strings.TrimPrefix(txResp, "Transaction started: ")

	e.Execute(`INSERT (k3, v3_tx) INTO mixed_table`)
	e.Execute(`UPDATE mixed_table SET (k2, v2_tx_updated)`)
	e.Execute(`DELETE k1 FROM mixed_table`)

	selectResp := e.Execute(`SELECT * FROM mixed_table`)
	expectedInTx := []string{
		fmt.Sprintf("k2: [tx_%s] v2_tx_updated", strings.TrimPrefix(txID, "tx_")),
		fmt.Sprintf("k3: [tx_%s] v3_tx", strings.TrimPrefix(txID, "tx_")),
	}

	for _, line := range expectedInTx {
		if !strings.Contains(selectResp, line) {
			t.Errorf("Expected SELECT in mixed transaction to contain %q, got:\n%s", line, selectResp)
		}
	}
	if strings.Contains(selectResp, "k1") {
		t.Errorf("Expected k1 to be deleted in tx, but it's present:\n%s", selectResp)
	}
	if strings.Contains(selectResp, "v2_orig") {
		t.Errorf("Expected v2_orig to be updated in tx, but old value is present:\n%s", selectResp)
	}
	if len(strings.Split(strings.TrimSpace(selectResp), "\n")) != len(expectedInTx) {
		t.Errorf("Expected %d lines in mixed transaction SELECT, got %d:\n%s", len(expectedInTx), len(strings.Split(strings.TrimSpace(selectResp), "\n")), selectResp)
	}

	e.Execute(`COMMIT`)

	selectResp = e.Execute(`SELECT * FROM mixed_table`)
	expectedAfterCommit := []string{
		"k2: v2_tx_updated",
		"k3: v3_tx",
	}

	for _, line := range expectedAfterCommit {
		if !strings.Contains(selectResp, line) {
			t.Errorf("Expected SELECT after mixed commit to contain %q, got:\n%s", line, selectResp)
		}
	}
	if strings.Contains(selectResp, "k1") {
		t.Errorf("Expected k1 to be permanently deleted, but it's present:\n%s", selectResp)
	}
	if len(strings.Split(strings.TrimSpace(selectResp), "\n")) != len(expectedAfterCommit) {
		t.Errorf("Expected %d lines after mixed commit, got %d:\n%s", len(expectedAfterCommit), len(strings.Split(strings.TrimSpace(selectResp), "\n")), selectResp)
	}
}

func TestEngineInvalidSyntax(t *testing.T) {
	e := setupTestEngine(t)

	resp := e.Execute(`INSERT INTO mytable`)
	if !strings.HasPrefix(resp, "Parse error:") {
		t.Fatalf("Expected parse error, got %q", resp)
	}
	resp = e.Execute(`INSERT (k,v) FROM mytable`)
	if !strings.HasPrefix(resp, "Parse error:") {
		t.Fatalf("Expected parse error, got %q", resp)
	}

	resp = e.Execute(`SELECT key1 FROM`)
	if !strings.HasPrefix(resp, "Parse error:") {
		t.Fatalf("Expected parse error for invalid SELECT, got %q", resp)
	}
	resp = e.Execute(`SELECT FROM mytable`)
	if !strings.HasPrefix(resp, "Parse error:") {
		t.Fatalf("Expected parse error for invalid SELECT, got %q", resp)
	}

	resp = e.Execute(`DELETE FROM mytable`)
	if !strings.HasPrefix(resp, "Parse error:") {
		t.Fatalf("Expected parse error for invalid DELETE, got %q", resp)
	}
	resp = e.Execute(`DELETE k1 mytable`)
	if !strings.HasPrefix(resp, "Parse error:") {
		t.Fatalf("Expected parse error for invalid DELETE, got %q", resp)
	}

	resp = e.Execute(`DROP`)
	if !strings.HasPrefix(resp, "Parse error:") {
		t.Fatalf("Expected parse error for invalid DROP, got %q", resp)
	}

	resp = e.Execute(`UPDATE mytable SET`)
	if !strings.HasPrefix(resp, "Parse error:") {
		t.Fatalf("Expected parse error, got %q", resp)
	}
	resp = e.Execute(`UPDATE mytable (k,v)`)
	if !strings.HasPrefix(resp, "Parse error:") {
		t.Fatalf("Expected parse error, got %q", resp)
	}
}

func TestEngineInsertOnlyNewKeys(t *testing.T) {
	e := setupTestEngine(t)

	e.Execute(`INSERT (key_x, val_x), (key_y, val_y) INTO insert_only_table`)

	resp := e.Execute(`INSERT (key_x, val_x_new), (key_z, val_z) INTO insert_only_table`)
	if resp != "Inserted 1 key(s) into table 'insert_only_table'" {
		t.Fatalf("Expected 'Inserted 1 key(s) into table 'insert_only_table'', got %q", resp)
	}

	resp = e.Execute(`SELECT key_x FROM insert_only_table`)
	if strings.TrimSpace(resp) != "key_x: val_x" {
		t.Fatalf("Expected key_x: val_x, got %q", resp)
	}

	resp = e.Execute(`SELECT key_z FROM insert_only_table`)
	if strings.TrimSpace(resp) != "key_z: val_z" {
		t.Fatalf("Expected key_z: val_z, got %q", resp)
	}
}
