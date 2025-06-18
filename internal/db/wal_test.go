package db

import (
	"os"
	"reflect"
	"testing"
)

func TestWAL_AppendAndReplay(t *testing.T) {
	path := "test_wal.log"
	defer os.Remove(path)

	wal := NewWAL(path)
	wal.Append("alpha", "1")
	wal.Append("beta", "2")
	wal.Append("gamma", "3")

	// Now delete one key
	wal.Delete("beta")

	entries, err := wal.Replay()
	if err != nil {
		t.Fatalf("Replay error: %v", err)
	}

	// Since beta was deleted, it should not appear
	expected := map[string]string{
		"alpha": "1",
		"gamma": "3",
	}

	if len(entries) != len(expected) {
		t.Fatalf("Expected %d entries, got %d", len(expected), len(entries))
	}

	// Convert entries slice to map for easier comparison
	resultMap := make(map[string]string)
	for _, entry := range entries {
		resultMap[entry[0]] = entry[1]
	}

	if !reflect.DeepEqual(resultMap, expected) {
		t.Errorf("Replay result = %v; want %v", resultMap, expected)
	}
}
