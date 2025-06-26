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

		wal.Append("", "table1", "keyA", "val1")
		wal.Append("", "table1", "keyB", "val2")
		wal.Append("", "table2", "keyX", "valX")
		wal.Delete("", "table1", "keyA") // Delete from table1

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

		resultMapTbl1 := make(map[string]string)
		if _, ok := replayedData["table1"]; ok {
			for _, entry := range replayedData["table1"] {
				resultMapTbl1[entry[0]] = entry[1]
			}
		}
		if !reflect.DeepEqual(resultMapTbl1, expectedTable1) {
			t.Errorf("table1: Replayed data mismatch. Got %v, expected %v", resultMapTbl1, expectedTable1)
		}

		resultMapTbl2 := make(map[string]string)
		if _, ok := replayedData["table2"]; ok {
			for _, entry := range replayedData["table2"] {
				resultMapTbl2[entry[0]] = entry[1]
			}
		}
		if !reflect.DeepEqual(resultMapTbl2, expectedTable2) {
			t.Errorf("table2: Replayed data mismatch. Got %v, expected %v", resultMapTbl2, expectedTable2)
		}
	})

	// --- Test Scenario 2: Overwriting a key ---
	t.Run("OverwriteKey", func(t *testing.T) {
		_ = os.Remove(path)
		wal := NewWAL(path)

		wal.Append("", "users", "user1", "Alice")
		wal.Append("", "users", "user1", "Bob") // Overwrite user1

		replayedData, err := wal.Replay()
		if err != nil {
			t.Fatalf("Replay error: %v", err)
		}

		expectedUsers := map[string]string{"user1": "Bob"}
		resultMapUsers := make(map[string]string)
		if _, ok := replayedData["users"]; ok {
			for _, entry := range replayedData["users"] {
				resultMapUsers[entry[0]] = entry[1]
			}
		}
		if !reflect.DeepEqual(resultMapUsers, expectedUsers) {
			t.Errorf("users: Replayed data mismatch. Got %v, expected %v", resultMapUsers, expectedUsers)
		}
	})

	// --- Test Scenario 3: Drop table ---
	t.Run("DropTable", func(t *testing.T) {
		_ = os.Remove(path)
		wal := NewWAL(path)

		wal.Append("", "items", "item1", "apple")
		wal.DropTable("", "items")
		wal.Append("", "products", "prod1", "laptop") // Should not be affected by items drop

		replayedData, err := wal.Replay()
		if err != nil {
			t.Fatalf("Replay error: %v", err)
		}

		if _, exists := replayedData["items"]; exists {
			t.Errorf("Expected 'items' table to be dropped, but it exists in replayed data: %v", replayedData["items"])
		}

		expectedProducts := map[string]string{"prod1": "laptop"}
		resultMapProducts := make(map[string]string)
		if _, ok := replayedData["products"]; ok {
			for _, entry := range replayedData["products"] {
				resultMapProducts[entry[0]] = entry[1]
			}
		}
		if !reflect.DeepEqual(resultMapProducts, expectedProducts) {
			t.Errorf("products: Replayed data mismatch. Got %v, expected %v", resultMapProducts, expectedProducts)
		}
	})

	// --- Test Scenario 4: Empty WAL ---
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

		wal.Append("", "tbl1", "k1", "v1")
		wal.Append("", "tbl2", "k2", "v2")
		wal.DropTable("", "tbl1")
		wal.Append("", "tbl1", "k3", "v3") // Re-create tbl1 after drop

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
			t.Errorf("tbl1: Replayed data mismatch. Got %v, expected %v", resultMapTbl1, expectedTbl1)
		}

		expectedTbl2 := map[string]string{"k2": "v2"}
		resultMapTbl2 := make(map[string]string)
		if _, ok := replayedData["tbl2"]; ok {
			for _, entry := range replayedData["tbl2"] {
				resultMapTbl2[entry[0]] = entry[1]
			}
		}
		if !reflect.DeepEqual(resultMapTbl2, expectedTbl2) {
			t.Errorf("tbl2: Replayed data mismatch. Got %v, expected %v", resultMapTbl2, expectedTbl2)
		}
	})
}

func TestWAL_Transactions(t *testing.T) {
	path := "test_wal_tx.log"
	defer os.Remove(path)

	t.Run("CommitTransaction", func(t *testing.T) {
		_ = os.Remove(path)
		wal := NewWAL(path)

		txID := "test_tx_1"
		wal.BeginTx(txID)
		wal.Append(txID, "tx_table", "k1", "v1")
		wal.Append(txID, "tx_table", "k2", "v2")
		wal.Delete(txID, "tx_table", "k1_del") // Should not exist yet, but for testing logic
		wal.CommitTx(txID)

		wal.Append("", "global_table", "gk1", "gv1") // Autocommit after tx

		replayedData, err := wal.Replay()
		if err != nil {
			t.Fatalf("Replay error: %v", err)
		}

		// Verify committed transaction data
		expectedTxTable := map[string]string{
			"k1": "v1",
			"k2": "v2",
		}
		resultMapTxTable := make(map[string]string)
		if _, ok := replayedData["tx_table"]; ok {
			for _, entry := range replayedData["tx_table"] {
				resultMapTxTable[entry[0]] = entry[1]
			}
		}
		if !reflect.DeepEqual(resultMapTxTable, expectedTxTable) {
			t.Errorf("tx_table: Replayed data mismatch after commit. Got %v, expected %v", resultMapTxTable, expectedTxTable)
		}

		// Verify global table data
		expectedGlobalTable := map[string]string{"gk1": "gv1"}
		resultMapGlobalTable := make(map[string]string)
		if _, ok := replayedData["global_table"]; ok {
			for _, entry := range replayedData["global_table"] {
				resultMapGlobalTable[entry[0]] = entry[1]
			}
		}
		if !reflect.DeepEqual(resultMapGlobalTable, expectedGlobalTable) {
			t.Errorf("global_table: Replayed data mismatch. Got %v, expected %v", resultMapGlobalTable, expectedGlobalTable)
		}
	})

	t.Run("RollbackTransaction", func(t *testing.T) {
		_ = os.Remove(path)
		wal := NewWAL(path)

		wal.Append("", "initial_table", "init_k", "init_v")

		txID := "test_tx_2"
		wal.BeginTx(txID)
		wal.Append(txID, "initial_table", "init_k", "updated_v_tx") // Update existing key
		wal.Append(txID, "new_tx_table", "tx_k1", "tx_v1")          // New table in tx
		wal.Delete(txID, "initial_table", "init_k")                 // Delete the updated key again
		wal.RollbackTx(txID)

		replayedData, err := wal.Replay()
		if err != nil {
			t.Fatalf("Replay error: %v", err)
		}

		// Verify initial_table reverted
		expectedInitialTable := map[string]string{"init_k": "init_v"}
		resultMapInitialTable := make(map[string]string)
		if _, ok := replayedData["initial_table"]; ok {
			for _, entry := range replayedData["initial_table"] {
				resultMapInitialTable[entry[0]] = entry[1]
			}
		}
		if !reflect.DeepEqual(resultMapInitialTable, expectedInitialTable) {
			t.Errorf("initial_table: Expected data to revert after rollback. Got %v, expected %v", resultMapInitialTable, expectedInitialTable)
		}

		// Verify new_tx_table does not exist
		if _, exists := replayedData["new_tx_table"]; exists {
			t.Errorf("Expected 'new_tx_table' not to exist after rollback, but it does: %v", replayedData["new_tx_table"])
		}
	})

	t.Run("TransactionWithDrop", func(t *testing.T) {
		_ = os.Remove(path)
		wal := NewWAL(path)

		wal.Append("", "pre_existing_table", "pk1", "pv1")

		txID := "test_tx_3"
		wal.BeginTx(txID)
		wal.DropTable(txID, "pre_existing_table")
		wal.Append(txID, "pre_existing_table", "pk2", "pv2_in_tx") // Re-create in same tx
		wal.CommitTx(txID)

		replayedData, err := wal.Replay()
		if err != nil {
			t.Fatalf("Replay error: %v", err)
		}

		// Verify that the table was dropped and then re-created with the new data
		expectedTable := map[string]string{"pk2": "pv2_in_tx"}
		resultMapTable := make(map[string]string)
		if _, ok := replayedData["pre_existing_table"]; ok {
			for _, entry := range replayedData["pre_existing_table"] {
				resultMapTable[entry[0]] = entry[1]
			}
		}
		if !reflect.DeepEqual(resultMapTable, expectedTable) {
			t.Errorf("pre_existing_table: Mismatch after drop and re-create in tx. Got %v, expected %v", resultMapTable, expectedTable)
		}
	})

	t.Run("RollbackTransactionWithDrop", func(t *testing.T) {
		_ = os.Remove(path)
		wal := NewWAL(path)

		wal.Append("", "original_table", "ok1", "ov1")

		txID := "test_tx_4"
		wal.BeginTx(txID)
		wal.DropTable(txID, "original_table")
		wal.Append(txID, "original_table", "ok2", "ov2_in_tx") // Re-create in same tx
		wal.RollbackTx(txID)

		replayedData, err := wal.Replay()
		if err != nil {
			t.Fatalf("Replay error: %v", err)
		}

		// Verify original_table reverted to its state before the transaction
		expectedTable := map[string]string{"ok1": "ov1"}
		resultMapTable := make(map[string]string)
		if _, ok := replayedData["original_table"]; ok {
			for _, entry := range replayedData["original_table"] {
				resultMapTable[entry[0]] = entry[1]
			}
		}
		if !reflect.DeepEqual(resultMapTable, expectedTable) {
			t.Errorf("original_table: Mismatch after rollback of drop and re-create. Got %v, expected %v", resultMapTable, expectedTable)
		}
	})

	t.Run("CommitAndDeleteExistingKeyInTx", func(t *testing.T) {
		_ = os.Remove(path)
		wal := NewWAL(path)
		wal.Append("", "items", "apple", "red")
		wal.Append("", "items", "banana", "yellow")

		txID := "test_tx_5"
		wal.BeginTx(txID)
		wal.Delete(txID, "items", "apple")
		wal.CommitTx(txID)

		replayedData, err := wal.Replay()
		if err != nil {
			t.Fatalf("Replay error: %v", err)
		}

		expectedItems := map[string]string{"banana": "yellow"}
		resultMapItems := make(map[string]string)
		if _, ok := replayedData["items"]; ok {
			for _, entry := range replayedData["items"] {
				resultMapItems[entry[0]] = entry[1]
			}
		}
		if !reflect.DeepEqual(resultMapItems, expectedItems) {
			t.Errorf("items: Mismatch after tx delete commit. Got %v, expected %v", resultMapItems, expectedItems)
		}
	})

	t.Run("RollbackAndDeleteExistingKeyInTx", func(t *testing.T) {
		_ = os.Remove(path)
		wal := NewWAL(path)
		wal.Append("", "fruits", "orange", "round")

		txID := "test_tx_6"
		wal.BeginTx(txID)
		wal.Delete(txID, "fruits", "orange")
		wal.RollbackTx(txID)

		replayedData, err := wal.Replay()
		if err != nil {
			t.Fatalf("Replay error: %v", err)
		}

		expectedFruits := map[string]string{"orange": "round"}
		resultMapFruits := make(map[string]string)
		if _, ok := replayedData["fruits"]; ok {
			for _, entry := range replayedData["fruits"] {
				resultMapFruits[entry[0]] = entry[1]
			}
		}
		if !reflect.DeepEqual(resultMapFruits, expectedFruits) {
			t.Errorf("fruits: Mismatch after tx delete rollback. Got %v, expected %v", resultMapFruits, expectedFruits)
		}
	})
}
