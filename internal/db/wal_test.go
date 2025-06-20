package db

import (
	"os"
	"reflect"
	"testing"
)

func TestWAL_AppendAndReplay(t *testing.T) {
	path := "test_wal.log"
	defer os.Remove(path) // Ensure log file is cleaned up after test

	// --- Test Scenario 1: Basic SET and DELETE operations across tables ---
	t.Run("BasicSetAndDelete", func(t *testing.T) {
		_ = os.Remove(path) // Clean log file for this sub-test
		wal := NewWAL(path)

		wal.Append("table1", "keyA", "val1")
		wal.Append("table1", "keyB", "val2")
		wal.Append("table2", "keyX", "valX")
		wal.Delete("table1", "keyA") // Delete from table1

		replayedData, err := wal.Replay()
		if err != nil {
			t.Fatalf("Replay error: %v", err)
		}

		expectedTable1 := map[string]string{
			"keyB": "val2",
		}
		expectedTable2 := map[string]string{
			"keyX": "valX",
		}

		if len(replayedData["table1"]) != len(expectedTable1) {
			t.Errorf("table1: Expected %d entries, got %d", len(expectedTable1), len(replayedData["table1"]))
		}
		if len(replayedData["table2"]) != len(expectedTable2) {
			t.Errorf("table2: Expected %d entries, got %d", len(expectedTable2), len(replayedData["table2"]))
		}

		resultMap1 := make(map[string]string)
		for _, entry := range replayedData["table1"] {
			resultMap1[entry[0]] = entry[1]
		}
		if !reflect.DeepEqual(resultMap1, expectedTable1) {
			t.Errorf("table1: Replay result = %v; want %v", resultMap1, expectedTable1)
		}

		resultMap2 := make(map[string]string)
		for _, entry := range replayedData["table2"] {
			resultMap2[entry[0]] = entry[1]
		}
		if !reflect.DeepEqual(resultMap2, expectedTable2) {
			t.Errorf("table2: Replay result = %v; want %v", resultMap2, expectedTable2)
		}
	})

	// --- Test Scenario 2: Overwriting a key ---
	t.Run("OverwriteKey", func(t *testing.T) {
		_ = os.Remove(path) // Clean log file for this sub-test
		wal := NewWAL(path)

		wal.Append("users", "user1", "Alice")
		wal.Append("users", "user1", "Bob") // Overwrite user1

		replayedData, err := wal.Replay()
		if err != nil {
			t.Fatalf("Replay error: %v", err)
		}

		expectedUsers := map[string]string{
			"user1": "Bob",
		}
		resultMapUsers := make(map[string]string)
		for _, entry := range replayedData["users"] {
			resultMapUsers[entry[0]] = entry[1]
		}

		if !reflect.DeepEqual(resultMapUsers, expectedUsers) {
			t.Errorf("Overwrite: Replay result = %v; want %v", resultMapUsers, expectedUsers)
		}
	})

	// --- Test Scenario 3: Drop Table ---
	t.Run("DropTable", func(t *testing.T) {
		_ = os.Remove(path) // Clean log file for this sub-test
		wal := NewWAL(path)

		wal.Append("tableA", "k1", "v1")
		wal.Append("tableB", "k2", "v2")
		wal.DropTable("tableA") // Drop tableA

		replayedData, err := wal.Replay()
		if err != nil {
			t.Fatalf("Replay error: %v", err)
		}

		if _, ok := replayedData["tableA"]; ok && len(replayedData["tableA"]) > 0 {
			t.Errorf("tableA should not exist or be empty after dropping, but it has %v", replayedData["tableA"])
		}
		expectedTableB := map[string]string{
			"k2": "v2",
		}
		resultMapB := make(map[string]string)
		for _, entry := range replayedData["tableB"] {
			resultMapB[entry[0]] = entry[1]
		}
		if !reflect.DeepEqual(resultMapB, expectedTableB) {
			t.Errorf("tableB: Replay result = %v; want %v", resultMapB, expectedTableB)
		}
	})

	// --- Test Scenario 4: Empty WAL file ---
	t.Run("EmptyWAL", func(t *testing.T) {
		_ = os.Remove(path) // Ensure no log file exists
		wal := NewWAL(path)

		replayedData, err := wal.Replay()
		if err != nil {
			t.Fatalf("Replay error on empty WAL: %v", err)
		}
		if len(replayedData) != 0 {
			t.Errorf("Expected empty map for empty WAL, got %v", replayedData)
		}
	})

	// --- Test Scenario 5: Mixed operations and re-creating a dropped table ---
	t.Run("MixedOperations", func(t *testing.T) {
		_ = os.Remove(path)
		wal := NewWAL(path)

		wal.Append("tbl1", "k1", "v1")
		wal.Append("tbl2", "k2", "v2")
		wal.DropTable("tbl1")
		wal.Append("tbl1", "k3", "v3") // Re-create tbl1 after drop

		replayedData, err := wal.Replay()
		if err != nil {
			t.Fatalf("Replay error: %v", err)
		}

		expectedTbl1 := map[string]string{"k3": "v3"}
		resultMapTbl1 := make(map[string]string)
		if _, ok := replayedData["tbl1"]; ok {
			for _, entry := range replayedData["tbl1"] {
				resultMapTbl1[entry[0]] = entry[1]
			}
		}
		if !reflect.DeepEqual(resultMapTbl1, expectedTbl1) {
			t.Errorf("tbl1 after re-creation: Replay result = %v; want %v", resultMapTbl1, expectedTbl1)
		}

		expectedTbl2 := map[string]string{"k2": "v2"}
		resultMapTbl2 := make(map[string]string)
		if _, ok := replayedData["tbl2"]; ok {
			for _, entry := range replayedData["tbl2"] {
				resultMapTbl2[entry[0]] = entry[1]
			}
		}
		if !reflect.DeepEqual(resultMapTbl2, expectedTbl2) {
			t.Errorf("tbl2: Replay result = %v; want %v", resultMapTbl2, expectedTbl2)
		}
	})
}
