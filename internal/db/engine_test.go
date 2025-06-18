package db

import (
	"os"
	"strings"
	"testing"
)

func TestEngine_Execute(t *testing.T) {
	path := "test_engine.log"
	defer os.Remove(path)

	eng := NewEngine(path)

	// SET and GET
	result := eng.Execute("SET a apple")
	if result != "OK" {
		t.Errorf("SET failed: %s", result)
	}

	result = eng.Execute("GET a")
	if result != "apple" {
		t.Errorf("GET a = %s; want apple", result)
	}

	result = eng.Execute("GET b")
	if result != "Key not found" {
		t.Errorf("GET b = %s; want Key not found", result)
	}

	result = eng.Execute("SET a apricot")
	if result != "OK" {
		t.Errorf("SET overwrite failed: %s", result)
	}

	result = eng.Execute("GET a")
	if result != "apricot" {
		t.Errorf("Updated GET a = %s; want apricot", result)
	}

	// DELETE
	result = eng.Execute("DELETE a")
	if result != "Deleted" {
		t.Errorf("DELETE failed: %s", result)
	}

	result = eng.Execute("GET a")
	if result != "Key not found" {
		t.Errorf("GET after DELETE = %s; want Key not found", result)
	}

	// SET multiple keys for SCAN
	eng.Execute("SET a alpha")
	eng.Execute("SET b beta")
	eng.Execute("SET c cherry")
	eng.Execute("SET d date")
	eng.Execute("SET e elderberry")

	result = eng.Execute("SCAN b d")
	expectedKeys := []string{"b: beta", "c: cherry", "d: date"}
	for _, expected := range expectedKeys {
		if !strings.Contains(result, expected) {
			t.Errorf("SCAN missing %q in result:\n%s", expected, result)
		}
	}

	if strings.Contains(result, "a:") || strings.Contains(result, "e:") {
		t.Errorf("SCAN result should not include 'a' or 'e':\n%s", result)
	}
}
