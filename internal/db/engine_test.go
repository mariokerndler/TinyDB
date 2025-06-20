package db

import (
	"os"
	"strings"
	"testing"
)

func setupTestEngine(t *testing.T) *Engine {
	t.Helper()

	logPath := "test_wal.log"
	_ = os.Remove(logPath) // clean old log if present

	engine := NewEngine(logPath)

	t.Cleanup(func() {
		os.Remove(logPath)
	})
	return engine
}

func TestEngineInsertAndSelectAll(t *testing.T) {
	e := setupTestEngine(t)

	insert := `INSERT INTO mytable VALUES (a, 1), (b, 2), (c, 3)`
	resp := e.Execute(insert)
	if resp != "OK" {
		t.Fatalf("Expected OK, got %q", resp)
	}

	selectAll := `SELECT * FROM mytable`
	resp = e.Execute(selectAll)
	expectedLines := []string{"a: 1", "b: 2", "c: 3"}

	for _, line := range expectedLines {
		if !strings.Contains(resp, line) {
			t.Errorf("Expected result to contain %q, got:\n%s", line, resp)
		}
	}
}

func TestEngineSelectSpecificKeys(t *testing.T) {
	e := setupTestEngine(t)

	insert := `INSERT INTO mytable VALUES (key1, value1), (key2, value2), (key3, value3)`
	resp := e.Execute(insert)
	if resp != "OK" {
		t.Fatalf("Expected OK, got %q", resp)
	}

	// Test selecting a single key
	selectOne := `SELECT key1 FROM mytable`
	resp = e.Execute(selectOne)
	expectedOne := "key1: value1"
	if !strings.Contains(resp, expectedOne) || strings.Contains(resp, "key2:") || strings.Contains(resp, "key3:") {
		t.Fatalf("Expected only %q, got:\n%s", expectedOne, resp)
	}

	// Test selecting multiple keys
	selectMultiple := `SELECT key1, key3 FROM mytable`
	resp = e.Execute(selectMultiple)
	expectedMultiple := []string{"key1: value1", "key3: value3"}
	for _, line := range expectedMultiple {
		if !strings.Contains(resp, line) {
			t.Errorf("Expected result to contain %q, got:\n%s", line, resp)
		}
	}
	if strings.Contains(resp, "key2:") {
		t.Errorf("Expected result NOT to contain key2, got:\n%s", resp)
	}

	// Test selecting a non-existent key
	selectNonExistent := `SELECT nonExistentKey FROM mytable`
	resp = e.Execute(selectNonExistent)
	if resp != "No results" {
		t.Fatalf("Expected 'No results' for non-existent key, got %q", resp)
	}

	// Test selecting a mix of existing and non-existing keys
	selectMixed := `SELECT key1, nonExistentKey FROM mytable`
	resp = e.Execute(selectMixed)
	expectedMixed := "key1: value1"
	if !strings.Contains(resp, expectedMixed) || strings.Contains(resp, "nonExistentKey:") {
		t.Fatalf("Expected only %q, got:\n%s", expectedMixed, resp)
	}
}

// TestEngineSelectWithWhere and TestEngineSelectMissingKey removed as WHERE clause is no longer supported for SELECT.

func TestEngineDelete(t *testing.T) {
	e := setupTestEngine(t)
	e.Execute(`INSERT INTO mytable VALUES (z, 99)`) // Changed (key, value) to VALUES as per typical INSERT syntax

	del := e.Execute(`DELETE FROM mytable WHERE key = z`)
	if del != "Deleted" {
		t.Fatalf("Expected 'Deleted', got %q", del)
	}

	// Select * to check if z is gone
	get := e.Execute(`SELECT * FROM mytable`)
	if strings.Contains(get, "z: 99") { // Check that 'z' is not in the results
		t.Fatalf("Expected 'z: 99' to be gone after delete, got %q", get)
	}
}

func TestEngineInvalidSyntax(t *testing.T) {
	e := setupTestEngine(t)

	resp := e.Execute(`INSERT INTO`)
	if !strings.HasPrefix(resp, "Parse error:") {
		t.Fatalf("Expected parse error, got %q", resp)
	}

	resp = e.Execute(`SELECT key1 FROM`) // Test invalid SELECT syntax
	if !strings.HasPrefix(resp, "Parse error:") {
		t.Fatalf("Expected parse error for invalid SELECT, got %q", resp)
	}
}

func TestEngineUnsupportedStatement(t *testing.T) {
	e := setupTestEngine(t)

	resp := e.Execute(`UPDATE table SET key = val`) // not supported
	if !strings.HasPrefix(resp, "Parse error:") {
		t.Fatalf("Expected parse error, got %q", resp)
	}
}

func TestEngineDropTable(t *testing.T) {
	e := setupTestEngine(t)

	e.Execute(`INSERT INTO users VALUES (u1, Alice), (u2, Bob)`)
	resp := e.Execute(`DROP TABLE users`)
	if resp != "Table dropped" {
		t.Fatalf("Expected 'Table dropped', got %q", resp)
	}

	resp = e.Execute(`SELECT * FROM users`)
	if resp != "No results" && resp != "" { // "" might be returned if nothing is found and trimmed
		t.Fatalf("Expected no results after drop, got %q", resp)
	}
}
