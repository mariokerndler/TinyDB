package db

import "fmt"

const ORDER = 4 // B+ Tree order - max children per internal node

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
