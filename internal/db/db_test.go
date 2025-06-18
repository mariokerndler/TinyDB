package db

import (
	"os"
	"testing"
)

func TestBPlusTree_InsertAndGet(t *testing.T) {
	tree := NewBPlusTree()

	tree.Insert("foo", "bar")
	tree.Insert("hello", "world")
	tree.Insert("abc", "123")
	tree.Insert("xyz", "789")

	tests := []struct {
		key      string
		expected string
		found    bool
	}{
		{"foo", "bar", true},
		{"hello", "world", true},
		{"abc", "123", true},
		{"xyz", "789", true},
		{"missing", "", false},
	}

	for _, tt := range tests {
		val, ok := tree.Get(tt.key)
		if ok != tt.found || val != tt.expected {
			t.Errorf("Get(%s) = %s, %v; want %s, %v", tt.key, val, ok, tt.expected, tt.found)
		}
	}
}

func TestWAL_AppendAndReplay(t *testing.T) {
	path := "test_wal.log"
	defer os.Remove(path)

	wal := NewWAL(path)
	wal.Append("alpha", "1")
	wal.Append("beta", "2")

	entries, err := wal.Replay()
	if err != nil {
		t.Fatalf("Replay error: %v", err)
	}

	expected := [][2]string{
		{"alpha", "1"},
		{"beta", "2"},
	}

	if len(entries) != len(expected) {
		t.Fatalf("Expected %d entries, got %d", len(expected), len(entries))
	}

	for i := range expected {
		if entries[i] != expected[i] {
			t.Errorf("Entry %d = %v; want %v", i, entries[i], expected[i])
		}
	}
}

func TestEngine_Execute(t *testing.T) {
	path := "test_engine.log"
	defer os.Remove(path)

	eng := NewEngine(path)

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
}
