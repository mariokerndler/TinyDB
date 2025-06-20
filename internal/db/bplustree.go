package db

import "fmt"

const ORDER = 4 // B+ Tree order - max children per internal node

// Minimum number of keys for a node to be valid (not underflowing)
const MIN_KEYS = (ORDER / 2) - 1 // For ORDER=4, MIN_KEYS = 1

type BPlusTree struct {
	root *BPlusTreeNode
}

type BPlusTreeNode struct {
	isLeaf   bool
	keys     []string
	children []*BPlusTreeNode // for internal nodes
	values   []string         // for leaf nodes
	next     *BPlusTreeNode   // leaf node chaining
}

func NewBPlusTree() *BPlusTree {
	// Initialize slices to avoid nil panics later
	leaf := &BPlusTreeNode{
		isLeaf: true,
		keys:   make([]string, 0, ORDER-1), // Pre-allocate capacity
		values: make([]string, 0, ORDER-1), // Pre-allocate capacity
	}
	return &BPlusTree{root: leaf}
}

// --- INSERT IMPLEMENTATION ---
func (t *BPlusTree) Insert(key, value string) {
	_, midKey, sibling := t.root.insert(key, value)

	if sibling != nil {
		// Root split: create a new root
		newRoot := &BPlusTreeNode{
			isLeaf:   false,
			keys:     make([]string, 0, ORDER-1),
			children: make([]*BPlusTreeNode, 0, ORDER),
		}
		newRoot.keys = append(newRoot.keys, midKey)
		newRoot.children = append(newRoot.children, t.root, sibling)
		t.root = newRoot
	}
}

// insert recursively inserts a key-value pair.
// It returns (promotedNode, promotedKey, newSibling)
// - promotedNode: always nil for now (can be used for more complex scenarios)
// - promotedKey: the key that needs to be promoted to the parent
// - newSibling: the new node created due to a split
func (n *BPlusTreeNode) insert(key, value string) (*BPlusTreeNode, string, *BPlusTreeNode) {
	if n.isLeaf {
		i := 0
		for i < len(n.keys) && n.keys[i] < key {
			i++
		}

		// If key already exists, update the value
		if i < len(n.keys) && n.keys[i] == key {
			n.values[i] = value
			return nil, "", nil // No split, no promotion
		}

		// Insert key and value at the correct position
		n.keys = append(n.keys[:i], append([]string{key}, n.keys[i:]...)...)
		n.values = append(n.values[:i], append([]string{value}, n.values[i:]...)...)

		// Check if split is needed
		if len(n.keys) < ORDER { // Node is not full
			return nil, "", nil
		}

		// Split the leaf node
		return n.splitLeaf()
	}

	// Internal node insert
	i := 0
	for i < len(n.keys) && key > n.keys[i] {
		i++
	}

	// Recursively insert into the appropriate child
	_, midKey, sibling := n.children[i].insert(key, value)
	if sibling == nil {
		return nil, "", nil // Child did not split
	}

	// Child split, insert promoted key and new sibling into current internal node
	n.keys = append(n.keys[:i], append([]string{midKey}, n.keys[i:]...)...)
	n.children = append(n.children[:i+1], append([]*BPlusTreeNode{sibling}, n.children[i+1:]...)...)

	// Check if this internal node needs to split
	if len(n.keys) < ORDER { // Node is not full (remember keys = ORDER -1, children = ORDER)
		return nil, "", nil
	}

	// Split the internal node
	return n.splitInternal()
}

func (n *BPlusTreeNode) splitLeaf() (*BPlusTreeNode, string, *BPlusTreeNode) {
	mid := len(n.keys) / 2

	// Initialize slices for the new sibling node
	sibling := &BPlusTreeNode{
		isLeaf: true,
		keys:   make([]string, 0, ORDER-1),
		values: make([]string, 0, ORDER-1),
		next:   n.next,
	}

	// Copy the latter half of keys and values to the sibling
	sibling.keys = append(sibling.keys, n.keys[mid:]...)
	sibling.values = append(sibling.values, n.values[mid:]...)

	// Truncate the original node's keys and values
	n.keys = n.keys[:mid]
	n.values = n.values[:mid]
	n.next = sibling

	// Promote the first key of the sibling
	return nil, sibling.keys[0], sibling
}

func (n *BPlusTreeNode) splitInternal() (*BPlusTreeNode, string, *BPlusTreeNode) {
	// Mid point for keys (remember, this key will be promoted)
	midKeyIndex := len(n.keys) / 2

	// Initialize slices for the new sibling node
	sibling := &BPlusTreeNode{
		isLeaf:   false,
		keys:     make([]string, 0, ORDER-1),
		children: make([]*BPlusTreeNode, 0, ORDER),
	}

	// The promoted key is the middle key
	promotedKey := n.keys[midKeyIndex]

	// Copy keys and children after the promoted key to the sibling
	sibling.keys = append(sibling.keys, n.keys[midKeyIndex+1:]...)
	sibling.children = append(sibling.children, n.children[midKeyIndex+1:]...)

	// Truncate the original node's keys and children
	n.keys = n.keys[:midKeyIndex]
	n.children = n.children[:midKeyIndex+1] // Important: children count is always one more than keys

	return nil, promotedKey, sibling
}

// --- END INSERT IMPLEMENTATION ---

// --- GET IMPLEMENTATION ---
func (t *BPlusTree) Get(key string) (string, bool) {
	node := t.root
	for !node.isLeaf {
		i := 0
		for i < len(node.keys) && key >= node.keys[i] { // Use >= for internal node traversal
			i++
		}
		node = node.children[i]
	}

	for i, k := range node.keys {
		if k == key {
			return node.values[i], true
		}
	}

	return "", false
}

// --- END GET IMPLEMENTATION ---

// --- DELETION IMPLEMENTATION ---
// Delete removes a key-value pair from the B+ Tree.
// It returns true if the element was successfully deleted, false otherwise.
func (t *BPlusTree) Delete(key string) bool {
	// Special case: Root is a leaf
	if t.root.isLeaf {
		deleted := t.root.deleteFromLeaf(key)
		// If root becomes empty after deletion, re-initialize to an empty leaf root
		if deleted && len(t.root.keys) == 0 {
			t.root = NewBPlusTree().root
		}
		return deleted
	}

	// Recursive deletion starting from the root
	// We need to pass a pointer to a boolean to track if a key was actually deleted anywhere in the subtree
	keyDeleted := false
	underflow := t.root.delete(key, nil, 0, &keyDeleted) // Pass keyDeleted by reference

	// If the root underflows and has only one child, that child becomes the new root
	if underflow && len(t.root.keys) == 0 {
		if len(t.root.children) == 1 {
			t.root = t.root.children[0]
		} else if len(t.root.children) == 0 { // Should only happen if the tree becomes completely empty
			t.root = NewBPlusTree().root // Tree became empty
		}
	}
	return keyDeleted
}

// delete recursively deletes a key from the node.
// Returns true if the node underflowed after deletion/merge.
// parent: the parent node (needed for redistribution/merge)
// childIndex: the index of 'n' in parent's children array
// keyDeleted: a pointer to a boolean indicating if the key was successfully deleted at any point
func (n *BPlusTreeNode) delete(key string, parent *BPlusTreeNode, childIndex int, keyDeleted *bool) bool {
	if n.isLeaf {
		deletedInLeaf := n.deleteFromLeaf(key)
		if deletedInLeaf {
			*keyDeleted = true // Mark that a key was deleted
		}
		return len(n.keys) < MIN_KEYS // Return true if leaf underflowed
	}

	// Internal node traversal
	i := 0
	for i < len(n.keys) && key >= n.keys[i] {
		i++
	}

	// Recursively delete from the child
	childUnderflow := n.children[i].delete(key, n, i, keyDeleted)

	if childUnderflow {
		return n.handleUnderflow(i) // Handle underflow of child at index i
	}
	return false // No underflow
}

// deleteFromLeaf removes a key from a leaf node.
// Returns true if the key was found and removed, false otherwise.
func (n *BPlusTreeNode) deleteFromLeaf(key string) bool {
	for i, k := range n.keys {
		if k == key {
			// Remove key and value
			n.keys = append(n.keys[:i], n.keys[i+1:]...)
			n.values = append(n.values[:i], n.values[i+1:]...)
			return true // Key found and removed
		}
	}
	return false // Key not found
}

// handleUnderflow attempts to redistribute or merge children.
// childIndex: the index of the child that underflowed.
// Returns true if this node (parent) also underflows after redistribution/merge.
func (n *BPlusTreeNode) handleUnderflow(childIndex int) bool {
	underflowingChild := n.children[childIndex]

	// Try to redistribute with left sibling
	if childIndex > 0 {
		leftSibling := n.children[childIndex-1]
		if len(leftSibling.keys) > MIN_KEYS {
			n.redistributeFromLeft(leftSibling, underflowingChild, childIndex-1)
			return false // Redistribution successful, no underflow
		}
	}

	// Try to redistribute with right sibling
	if childIndex < len(n.children)-1 {
		rightSibling := n.children[childIndex+1]
		if len(rightSibling.keys) > MIN_KEYS {
			n.redistributeFromRight(underflowingChild, rightSibling, childIndex)
			return false // Redistribution successful, no underflow
		}
	}

	// If redistribution not possible, merge
	if childIndex > 0 { // Merge with left sibling
		n.merge(n.children[childIndex-1], underflowingChild, childIndex-1)
	} else { // Merge with right sibling (must have one if childIndex is 0 and no left sibling)
		n.merge(underflowingChild, n.children[childIndex+1], childIndex)
	}

	// After merge, check if this parent node underflows
	return len(n.keys) < MIN_KEYS
}

// redistributeFromLeft borrows a key/value/child from the leftSibling to the underflowingChild.
// leftSibling: the donor (left sibling)
// underflowingChild: the receiver (underflowing child)
// separatorIndex: the index of the separator key in parent that separates leftSibling and underflowingChild
func (n *BPlusTreeNode) redistributeFromLeft(leftSibling, underflowingChild *BPlusTreeNode, separatorIndex int) {
	if underflowingChild.isLeaf {
		// Move last key/value from leftSibling to underflowingChild
		keyToMove := leftSibling.keys[len(leftSibling.keys)-1]
		valueToMove := leftSibling.values[len(leftSibling.values)-1]
		leftSibling.keys = leftSibling.keys[:len(leftSibling.keys)-1]
		leftSibling.values = leftSibling.values[:len(leftSibling.values)-1]

		underflowingChild.keys = append([]string{keyToMove}, underflowingChild.keys...)
		underflowingChild.values = append([]string{valueToMove}, underflowingChild.values...)

		// Update parent's separator key: it should be the new first key of the now-augmented underflowingChild
		n.keys[separatorIndex] = underflowingChild.keys[0]
	} else { // Internal node redistribution
		// Pull down parent's separator key
		promotedKey := n.keys[separatorIndex]
		n.keys[separatorIndex] = leftSibling.keys[len(leftSibling.keys)-1] // Replace with last key from left sibling
		leftSibling.keys = leftSibling.keys[:len(leftSibling.keys)-1]

		// Move key and last child from left sibling to underflowing child
		childToMove := leftSibling.children[len(leftSibling.children)-1]
		leftSibling.children = leftSibling.children[:len(leftSibling.children)-1]

		underflowingChild.keys = append([]string{promotedKey}, underflowingChild.keys...)
		underflowingChild.children = append([]*BPlusTreeNode{childToMove}, underflowingChild.children...)
	}
}

// redistributeFromRight borrows a key/value/child from the rightSibling to the underflowingChild.
// underflowingChild: the receiver (underflowing child)
// rightSibling: the donor (right sibling)
// separatorIndex: the index of the separator key in parent that separates underflowingChild and rightSibling
func (n *BPlusTreeNode) redistributeFromRight(underflowingChild, rightSibling *BPlusTreeNode, separatorIndex int) {
	if underflowingChild.isLeaf {
		// Take first key/value from rightSibling, add to end of underflowingChild
		keyToMove := rightSibling.keys[0]
		valueToMove := rightSibling.values[0]
		rightSibling.keys = rightSibling.keys[1:]
		rightSibling.values = rightSibling.values[1:]

		underflowingChild.keys = append(underflowingChild.keys, keyToMove)
		underflowingChild.values = append(underflowingChild.values, valueToMove)

		// Update parent's separator key: it should be the new first key of the (now-reduced) rightSibling
		n.keys[separatorIndex] = rightSibling.keys[0]
	} else { // Internal node redistribution
		// Pull down parent's separator key
		promotedKey := n.keys[separatorIndex]
		n.keys[separatorIndex] = rightSibling.keys[0] // Replace with first key from right sibling
		rightSibling.keys = rightSibling.keys[1:]

		// Move key and first child from right sibling to underflowing child
		childToMove := rightSibling.children[0]
		rightSibling.children = rightSibling.children[1:]

		underflowingChild.keys = append(underflowingChild.keys, promotedKey)
		underflowingChild.children = append(underflowingChild.children, childToMove)
	}
}

// merge merges two sibling nodes.
// sibling1: the first sibling (will contain merged content)
// sibling2: the second sibling (will be removed)
// separatorIndex: the index of the key in parent that separates sibling1 and sibling2
func (n *BPlusTreeNode) merge(sibling1, sibling2 *BPlusTreeNode, separatorIndex int) {
	if sibling1.isLeaf {
		sibling1.keys = append(sibling1.keys, sibling2.keys...)
		sibling1.values = append(sibling1.values, sibling2.values...)
		sibling1.next = sibling2.next // Crucial: Update leaf chaining
	} else { // Internal node merge
		// Pull down the separator key from the parent
		promotedKey := n.keys[separatorIndex]
		sibling1.keys = append(sibling1.keys, promotedKey) // Key from parent goes into sibling1
		sibling1.keys = append(sibling1.keys, sibling2.keys...)
		sibling1.children = append(sibling1.children, sibling2.children...)
	}

	// Remove the separator key and the second sibling from the parent
	n.keys = append(n.keys[:separatorIndex], n.keys[separatorIndex+1:]...)
	n.children = append(n.children[:separatorIndex+1], n.children[separatorIndex+2:]...) // Remove sibling2
}

// --- END DELETION IMPLEMENTATION ---

// --- RANGE QUERY/SCAN IMPLEMENTATION ---
func (t *BPlusTree) RangeQuery(startKey, endKey string) map[string]string {
	results := make(map[string]string)
	if t.root == nil {
		return results
	}

	node := t.root
	// Find leftmost leaf
	for !node.isLeaf {
		node = node.children[0]
	}
	for node != nil {
		for i, k := range node.keys {
			if (startKey == "" || k >= startKey) && (endKey == "" || k <= endKey) {
				results[k] = node.values[i]
			}
		}
		node = node.next
	}
	return results
}

// --- END RANGE QUERY/SCAN IMPLEMENTATION ---

// --- PrintTree IMPLEMENTATION ---
func (t *BPlusTree) PrintTree() {
	var levels [][]string
	var collect func(n *BPlusTreeNode, level int)
	collect = func(n *BPlusTreeNode, level int) {
		if len(levels) <= level {
			levels = append(levels, []string{})
		}
		levels[level] = append(levels[level], fmt.Sprintf("[%v]", n.keys))
		if !n.isLeaf {
			for _, c := range n.children {
				collect(c, level+1)
			}
		}
	}
	collect(t.root, 0)
	for i, lvl := range levels {
		fmt.Printf("Level %d: %s\n", i, lvl)
	}
}

// --- END PrintTree IMPLEMENTATION ---
