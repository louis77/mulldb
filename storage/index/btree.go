package index

const btreeOrder = 64 // max children per node

// BTree is an in-memory B-tree that maps unique keys to row IDs.
// It implements the Index interface.
type BTree struct {
	root *btreeNode
	cmp  func(a, b any) int
}

// NewBTree creates a new B-tree using the given comparator.
// The comparator must return -1, 0, or 1 (negative values other than -1 are
// treated as "incomparable" and should not occur for indexed keys).
func NewBTree(cmp func(a, b any) int) *BTree {
	return &BTree{cmp: cmp}
}

type btreeEntry struct {
	key   any
	rowID int64
}

type btreeNode struct {
	entries  []btreeEntry
	children []*btreeNode
}

func (n *btreeNode) isLeaf() bool {
	return len(n.children) == 0
}

// search returns the index where key should be inserted in n.entries.
// If found is true, entries[idx].key == key.
func (b *BTree) search(n *btreeNode, key any) (idx int, found bool) {
	lo, hi := 0, len(n.entries)
	for lo < hi {
		mid := (lo + hi) / 2
		c := b.cmp(key, n.entries[mid].key)
		if c == 0 {
			return mid, true
		}
		if c < 0 {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo, false
}

// Put inserts key→rowID. Returns false if the key already exists.
func (b *BTree) Put(key any, rowID int64) bool {
	if b.root == nil {
		b.root = &btreeNode{
			entries: []btreeEntry{{key: key, rowID: rowID}},
		}
		return true
	}

	// Check if key exists.
	if _, found := b.get(b.root, key); found {
		return false
	}

	// Insert into tree, splitting as needed.
	newEntry, newChild := b.insert(b.root, btreeEntry{key: key, rowID: rowID})
	if newChild != nil {
		// Root was split — create new root.
		b.root = &btreeNode{
			entries:  []btreeEntry{newEntry},
			children: []*btreeNode{b.root, newChild},
		}
	}
	return true
}

// Get looks up a key. Returns the rowID and true if found.
func (b *BTree) Get(key any) (int64, bool) {
	if b.root == nil {
		return 0, false
	}
	return b.get(b.root, key)
}

func (b *BTree) get(n *btreeNode, key any) (int64, bool) {
	idx, found := b.search(n, key)
	if found {
		return n.entries[idx].rowID, true
	}
	if n.isLeaf() {
		return 0, false
	}
	return b.get(n.children[idx], key)
}

// Delete removes a key. Returns false if the key was not found.
func (b *BTree) Delete(key any) bool {
	if b.root == nil {
		return false
	}
	deleted := b.delete(b.root, key)
	if !deleted {
		return false
	}
	// Shrink root if it has no entries but has a child.
	if len(b.root.entries) == 0 && !b.root.isLeaf() {
		b.root = b.root.children[0]
	}
	// Empty tree.
	if len(b.root.entries) == 0 {
		b.root = nil
	}
	return true
}

// insert descends into n and inserts e. If a split occurs, it returns
// the promoted entry and the new right child. Otherwise newChild is nil.
func (b *BTree) insert(n *btreeNode, e btreeEntry) (promoted btreeEntry, newChild *btreeNode) {
	idx, _ := b.search(n, e.key)

	if n.isLeaf() {
		// Insert into entries at idx.
		n.entries = append(n.entries, btreeEntry{})
		copy(n.entries[idx+1:], n.entries[idx:])
		n.entries[idx] = e
	} else {
		// Recurse into child.
		promoted, newChild = b.insert(n.children[idx], e)
		if newChild == nil {
			return btreeEntry{}, nil
		}
		// Insert promoted entry and newChild into this node.
		n.entries = append(n.entries, btreeEntry{})
		copy(n.entries[idx+1:], n.entries[idx:])
		n.entries[idx] = promoted

		n.children = append(n.children, nil)
		copy(n.children[idx+2:], n.children[idx+1:])
		n.children[idx+1] = newChild

		newChild = nil // reset — we'll check if this node needs splitting below
	}

	// Split if overflowed.
	maxEntries := btreeOrder - 1
	if len(n.entries) > maxEntries {
		return b.split(n)
	}
	return btreeEntry{}, nil
}

// split splits n at the median, returning the promoted entry and the new right node.
// n is modified in place to become the left node.
func (b *BTree) split(n *btreeNode) (btreeEntry, *btreeNode) {
	mid := len(n.entries) / 2
	promoted := n.entries[mid]

	right := &btreeNode{
		entries: make([]btreeEntry, len(n.entries[mid+1:])),
	}
	copy(right.entries, n.entries[mid+1:])

	if !n.isLeaf() {
		right.children = make([]*btreeNode, len(n.children[mid+1:]))
		copy(right.children, n.children[mid+1:])
		n.children = n.children[:mid+1]
	}

	n.entries = n.entries[:mid]
	return promoted, right
}

// delete removes key from the subtree rooted at n. Returns true if found.
// Uses a simplified approach: after deletion, nodes may temporarily underflow.
// For an in-memory index this is acceptable since we optimize for simplicity.
func (b *BTree) delete(n *btreeNode, key any) bool {
	idx, found := b.search(n, key)

	if n.isLeaf() {
		if !found {
			return false
		}
		// Remove entry at idx.
		n.entries = append(n.entries[:idx], n.entries[idx+1:]...)
		return true
	}

	if found {
		// Replace with in-order predecessor (largest key in left subtree).
		pred := b.largest(n.children[idx])
		n.entries[idx] = pred
		return b.delete(n.children[idx], pred.key)
	}

	// Recurse into child.
	return b.delete(n.children[idx], key)
}

// largest returns the rightmost entry in the subtree rooted at n.
func (b *BTree) largest(n *btreeNode) btreeEntry {
	for !n.isLeaf() {
		n = n.children[len(n.children)-1]
	}
	return n.entries[len(n.entries)-1]
}

// -------------------------------------------------------------------------
// MultiBTree — non-unique index (key → multiple row IDs)
// -------------------------------------------------------------------------

// multiKey wraps a user key and rowID into a composite key so that every
// entry in the underlying B-tree is unique even when user keys repeat.
type multiKey struct {
	key   any
	rowID int64
}

// MultiBTree is an in-memory B-tree that maps non-unique keys to row IDs.
// It implements the MultiIndex interface. Internally it stores composite
// (key, rowID) entries in a standard BTree, using a comparator that orders
// first by key, then by rowID.
type MultiBTree struct {
	bt  *BTree
	cmp func(a, b any) int // user-supplied key comparator
}

// NewMultiBTree creates a new multi-value B-tree using the given key comparator.
func NewMultiBTree(cmp func(a, b any) int) *MultiBTree {
	compositeCmp := func(a, b any) int {
		ak, bk := a.(multiKey), b.(multiKey)
		c := cmp(ak.key, bk.key)
		if c != 0 {
			return c
		}
		switch {
		case ak.rowID < bk.rowID:
			return -1
		case ak.rowID > bk.rowID:
			return 1
		default:
			return 0
		}
	}
	return &MultiBTree{bt: NewBTree(compositeCmp), cmp: cmp}
}

// Put inserts a key→rowID mapping. Always succeeds because the composite
// (key, rowID) is unique even when the user key repeats.
func (m *MultiBTree) Put(key any, rowID int64) {
	m.bt.Put(multiKey{key: key, rowID: rowID}, rowID)
}

// GetAll returns all row IDs associated with the given key, in rowID order.
func (m *MultiBTree) GetAll(key any) []int64 {
	if m.bt.root == nil {
		return nil
	}
	var result []int64
	m.collectAll(m.bt.root, key, &result)
	return result
}

// Delete removes a specific (key, rowID) pair. Returns false if not found.
func (m *MultiBTree) Delete(key any, rowID int64) bool {
	return m.bt.Delete(multiKey{key: key, rowID: rowID})
}

// collectAll performs an in-order traversal of the subtree rooted at n,
// collecting row IDs for all entries whose user key matches key.
// Because entries are sorted by (key, rowID), all matching entries are
// contiguous and the traversal can prune branches.
func (m *MultiBTree) collectAll(n *btreeNode, key any, result *[]int64) {
	for i, e := range n.entries {
		ek := e.key.(multiKey)
		c := m.cmp(ek.key, key)
		switch {
		case c > 0:
			// Entry is past the search key. Only the left child may
			// still contain matches.
			if !n.isLeaf() {
				m.collectAll(n.children[i], key, result)
			}
			return
		case c == 0:
			// Left child may have earlier matches (smaller rowID).
			if !n.isLeaf() {
				m.collectAll(n.children[i], key, result)
			}
			*result = append(*result, e.rowID)
		}
		// c < 0: entry key < search key — left child is all < entry,
		// so no matches there. Continue to next entry.
	}
	// Rightmost child may contain matches.
	if !n.isLeaf() {
		m.collectAll(n.children[len(n.children)-1], key, result)
	}
}
