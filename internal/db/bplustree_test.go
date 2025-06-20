package db

import (
	"testing"
)

func TestInsertAndGet(t *testing.T) {
	tree := NewBPlusTree()
	tree.Insert("a", "apple")
	tree.Insert("b", "banana")
	tree.Insert("c", "cherry")

	tests := []struct {
		key      string
		expected string
		found    bool
	}{
		{"a", "apple", true},
		{"b", "banana", true},
		{"c", "cherry", true},
		{"x", "", false}, // nonexistent
	}

	for _, tt := range tests {
		val, ok := tree.Get(tt.key)
		if ok != tt.found || val != tt.expected {
			t.Errorf("Get(%q) = (%q, %v), expected (%q, %v)", tt.key, val, ok, tt.expected, tt.found)
		}
	}
}

func TestUpdateValue(t *testing.T) {
	tree := NewBPlusTree()
	tree.Insert("a", "apple")
	tree.Insert("a", "apricot") // update value

	val, ok := tree.Get("a")
	if !ok || val != "apricot" {
		t.Errorf("Expected updated value 'apricot', got %q", val)
	}
}

func TestDelete(t *testing.T) {
	tree := NewBPlusTree()
	tree.Insert("a", "alpha")
	tree.Insert("b", "beta")
	tree.Insert("c", "charlie")
	tree.Delete("b")

	_, ok := tree.Get("b")
	if ok {
		t.Error("Expected key 'b' to be deleted")
	}

	val, ok := tree.Get("a")
	if !ok || val != "alpha" {
		t.Error("Key 'a' should still exist")
	}
}

func TestDeleteNonExistentKey(t *testing.T) {
	tree := NewBPlusTree()
	tree.Insert("a", "alpha")
	tree.Delete("z") // non-existent key, should be safe

	val, ok := tree.Get("a")
	if !ok || val != "alpha" {
		t.Error("Key 'a' should not be affected by deletion of non-existent key")
	}
}

func TestRangeQuery(t *testing.T) {
	tree := NewBPlusTree()
	tree.Insert("a", "apple")
	tree.Insert("b", "banana")
	tree.Insert("c", "cherry")
	tree.Insert("d", "date")
	tree.Insert("e", "elderberry")

	result := tree.RangeQuery("b", "d")
	expected := map[string]string{
		"b": "banana",
		"c": "cherry",
		"d": "date",
	}

	if len(result) != len(expected) {
		t.Fatalf("Expected %d results, got %d", len(expected), len(result))
	}

	for k, v := range expected {
		if result[k] != v {
			t.Errorf("Expected key %q = %q, got %q", k, v, result[k])
		}
	}
}

func TestInsertSplitRoot(t *testing.T) {
	tree := NewBPlusTree()
	keys := []string{"d", "b", "a", "c", "e"} // Will cause multiple splits

	for _, k := range keys {
		tree.Insert(k, k+"-val")
	}

	for _, k := range keys {
		val, ok := tree.Get(k)
		if !ok || val != k+"-val" {
			t.Errorf("Get(%q) = %q, want %q", k, val, k+"-val")
		}
	}
}

func TestUnderflowRedistribution(t *testing.T) {
	tree := NewBPlusTree()

	// Fill tree enough to force internal node with redistribution possibility
	// This will split at least once: ORDER = 4 → max 3 keys per node
	keys := []string{"a", "b", "c", "d", "e", "f", "g"}
	for _, k := range keys {
		tree.Insert(k, k+"-val")
	}

	// Delete a key to force leaf underflow
	tree.Delete("b") // should be handled by redistribution

	// Validate remaining keys
	expected := map[string]string{
		"a": "a-val",
		"c": "c-val",
		"d": "d-val",
		"e": "e-val",
		"f": "f-val",
		"g": "g-val",
	}

	for k, v := range expected {
		val, ok := tree.Get(k)
		if !ok || val != v {
			t.Errorf("After redistribution: Get(%q) = %q, want %q", k, val, v)
		}
	}
}

func TestUnderflowMerge(t *testing.T) {
	tree := NewBPlusTree()

	// Insert minimal number of keys to force merge scenario
	keys := []string{"a", "b", "c", "d"}
	for _, k := range keys {
		tree.Insert(k, k+"-val")
	}

	// Delete to underflow one leaf and force merge
	tree.Delete("a")
	tree.Delete("b")

	// Now we expect "c" and "d" to still exist
	expected := map[string]string{
		"c": "c-val",
		"d": "d-val",
	}

	for k, v := range expected {
		val, ok := tree.Get(k)
		if !ok || val != v {
			t.Errorf("After merge: Get(%q) = %q, want %q", k, val, v)
		}
	}

	// And deleted keys should not be found
	for _, k := range []string{"a", "b"} {
		if _, ok := tree.Get(k); ok {
			t.Errorf("Expected key %q to be deleted", k)
		}
	}
}
