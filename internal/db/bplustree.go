package db

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

func NewBTree() *BPlusTree {
	leaf := &BPlusTreeNode{isLeaf: true}

	return &BPlusTree{root: leaf}
}

func (t *BPlusTree) Insert(key, value string) {
	newRoot, _, _ := t.root.insert(key, value)
	if newRoot != nil {
		t.root = newRoot
	}
}

func (n *BPlusTreeNode) insert(key, value string) (*BPlusTreeNode, string, *BPlusTreeNode) {
	if n.isLeaf {
		i := 0
		for i < len(n.keys) && n.keys[i] < key {
			i++
		}

		if i < len(n.keys) && n.keys[i] == key {
			n.values[i] = value
			return nil, "", nil
		}

		n.keys = append(n.keys[:i], append([]string{key}, n.keys[i:]...)...)
		n.values = append(n.values[:i], append([]string{value}, n.values[i:]...)...)

		if len(n.keys) < ORDER {
			return nil, "", nil
		}

		return n.splitLeaf()
	}

	// Internal node insert
	i := 0
	for i < len(n.keys) && key > n.keys[i] {
		i++
	}

	newChild, midKey, sibling := n.children[i].insert(key, value)
	if newChild != nil {
		return nil, "", nil
	}

	// Insert new child and key into internal node
	n.keys = append(n.keys[:i], append([]string{midKey}, n.keys[i:]...)...)
	n.children = append(n.children[:i+1], append([]*BPlusTreeNode{sibling}, n.children[i+1:]...)...)

	if len(n.keys) < ORDER {
		return nil, "", nil
	}

	return n.splitInternal()
}

func (t *BPlusTree) Get(key string) (string, bool) {
	node := t.root
	for !node.isLeaf {
		i := 0
		for i < len(node.keys) && key > node.keys[i] {
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

func (n *BPlusTreeNode) splitLeaf() (*BPlusTreeNode, string, *BPlusTreeNode) {
	mid := len(n.keys) / 2
	sibling := &BPlusTreeNode{
		isLeaf: true,
		keys:   append([]string{}, n.keys[mid:]...),
		values: append([]string{}, n.values[mid:]...),
		next:   n.next,
	}

	n.keys = n.keys[:mid]
	n.values = n.values[:mid]
	n.next = sibling

	return &BPlusTreeNode{
		keys:     []string{sibling.keys[0]},
		children: []*BPlusTreeNode{n, sibling},
	}, sibling.keys[0], sibling
}

func (n *BPlusTreeNode) splitInternal() (*BPlusTreeNode, string, *BPlusTreeNode) {
	mid := len(n.keys) / 2
	sibling := &BPlusTreeNode{
		isLeaf:   false,
		keys:     append([]string{}, n.keys[mid+1:]...),
		children: append([]*BPlusTreeNode{}, n.children[mid+1:]...),
	}

	newRoot := &BPlusTreeNode{
		isLeaf:   false,
		keys:     []string{n.keys[mid]},
		children: []*BPlusTreeNode{n, sibling},
	}

	n.keys = n.keys[:mid]
	n.children = n.children[:mid+1]

	return newRoot, n.keys[mid], sibling
}
