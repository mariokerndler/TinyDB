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

func TestEngineSelectWithWhere(t *testing.T) {
	e := setupTestEngine(t)
	e.Execute(`INSERT INTO mytable VALUES (foo, bar)`)

	resp := e.Execute(`SELECT * FROM mytable WHERE key = foo`)
	if resp != "bar" {
		t.Fatalf("Expected 'bar', got %q", resp)
	}
}

func TestEngineSelectMissingKey(t *testing.T) {
	e := setupTestEngine(t)
	resp := e.Execute(`SELECT * FROM mytable WHERE key = nope`)
	if resp != "Key not found" {
		t.Fatalf("Expected 'Key not found', got %q", resp)
	}
}

func TestEngineDelete(t *testing.T) {
	e := setupTestEngine(t)
	e.Execute(`INSERT INTO mytable (key, value) VALUES (z, 99)`)

	del := e.Execute(`DELETE FROM mytable WHERE key = z`)
	if del != "Deleted" {
		t.Fatalf("Expected 'Deleted', got %q", del)
	}

	get := e.Execute(`SELECT * FROM mytable WHERE key = z`)
	if get != "Key not found" {
		t.Fatalf("Expected 'Key not found' after delete, got %q", get)
	}
}

func TestEngineInvalidSyntax(t *testing.T) {
	e := setupTestEngine(t)

	resp := e.Execute(`INSERT INTO`)
	if !strings.HasPrefix(resp, "Parse error:") {
		t.Fatalf("Expected parse error, got %q", resp)
	}
}

func TestEngineUnsupportedStatement(t *testing.T) {
	e := setupTestEngine(t)

	// Force a bad type (simulated manually)
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
	if resp != "No results" {
		t.Fatalf("Expected no results after drop, got %q", resp)
	}
}
