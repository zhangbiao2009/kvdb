package btreedb

import (
	"btreedb/utils"
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/edsrzf/mmap-go"
)

const (
	BLOCK_SIZE       = 4096
	BLOCK_MAGIC      = "BLKSTART"
	BLOCK_MAGIC_SIZE = len(BLOCK_MAGIC)
)

var ERR_KEY_NOT_EXIST error = errors.New("key not exist")

type Offset int64

type DB struct {
	metaBlock  *utils.MetaBlock
	blockMgr   *BlockMgr
	maxKeySize int
	file       *os.File
	mmap       mmap.MMap
	nUsedBlock int
}

// 创建DB文件，并且格式化
func CreateDB(filePath string, maxKeySize int, degree int) (*DB, error) {
	defaultDBSize := 20 * 1024 * 1024                                       // 20M
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644) // 打开的同时，如果file存在的话，O_TRUNC会清空file
	if err != nil {
		return nil, err
	}
	err = f.Truncate(int64(defaultDBSize)) // 为简化编程，预先分配好空间，不再扩容
	if err != nil {
		f.Close()
		return nil, err
	}
	mmap, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		f.Close()
		return nil, err
	}
	db := &DB{
		maxKeySize: maxKeySize,
		file:       f,
		mmap:       mmap,
		blockMgr:   NewBlockMgr(mmap),
	}
	db.Init(degree)
	return db, nil
}

func OpenDB(filePath string) (*DB, error) {
	f, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	mmap, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		f.Close()
		return nil, err
	}
	db := &DB{
		file: f,
		mmap: mmap,
	}
	db.loadMetaBlock()
	return db, nil
}

// 初始化DB，构造meta block和root block
func (db *DB) Init(degree int) {
	db.metaBlock = db.blockMgr.buildMetaBlock(degree)
	db.blockMgr.setMetaBlock(db.metaBlock)
	root := newLeafNode(db) // for btree root node
	_ = root
}

func (db *DB) Insert(key, val []byte) error {
	meta := db.metaBlock
	rootBlockId := int(*meta.RootBlockId())
	promotedKey, rightSibling, err := db.insert(rootBlockId, key, val)
	if err != nil {
		return err
	}
	if rightSibling == nil {
		return nil
	}
	newRoot := newInternalNode(db)
	newRoot.insertKeyInPos(0, promotedKey)
	newRoot.setNKeys(1)
	newRoot.setChildBlockId(0, rootBlockId)
	newRoot.setChildBlockId(1, rightSibling.blockId)
	meta.MutateRootBlockId(uint32(newRoot.blockId))
	return nil
}

func (db *DB) Find(key []byte) ([]byte, error) {
	return db.find(int(*db.metaBlock.RootBlockId()), key)
}

func (db *DB) Traverse(f func(*Node) bool) {
	db.traverse(int(*db.metaBlock.RootBlockId()), f)
}
func (db *DB) traverse(blockId int, f func(*Node) bool) bool {
	node := loadNode(db, blockId)
	exit := f(node)
	if exit {
		return true
	}
	if node.isLeaf() {
		return false
	}
	i := 0
	for ; i <= node.nKeys(); i++ {
		childBlockId := node.getChildBlockId(i)
		if db.traverse(childBlockId, f) {
			return true
		}
	}
	return false
}

func (db *DB) PrintDebugInfo() {
	db.printDebugInfo(int(*db.metaBlock.RootBlockId()))
}

func (db *DB) PrintDotGraph(w io.Writer) {
	fmt.Fprintln(w, "digraph g {")
	fmt.Fprintln(w, "node [shape = record,height=.1];")
	db.printDotGraph(w, int(*db.metaBlock.RootBlockId()))
	fmt.Fprintln(w, "}")
}

func (db *DB) PrintDotGraph2(fileName string) {
	f, err := os.Create(fileName)
	if err != nil {
		log.Fatal(err)
	}
	bw := bufio.NewWriter(f)
	db.PrintDotGraph(bw)
	bw.Flush()
	f.Close()
}

func (db *DB) printDotGraph(w io.Writer, blockId int) {
	node := loadNode(db, blockId)
	if node.isLeaf() {
		node.printDotGraphLeaf(w)
		return
	}
	// print internal node
	fmt.Fprintf(w, "node%d [label = \"", node.blockId)
	fmt.Fprintf(w, "<f0> ") // for the ptr[0]
	for i := 0; i < node.nKeys(); i++ {
		fmt.Fprintf(w, "|<f%d> %v", 2*i+1, string(node.getKey(i))) // for key i
		fmt.Fprintf(w, "|<f%d> ", 2*(i+1))                         // for ptr i+1
	}
	fmt.Fprintln(w, "\"];")

	for i := 0; i <= node.nKeys(); i++ {
		childBlockId := node.getChildBlockId(i)
		fid := 2 * i
		fmt.Fprintf(w, "\"node%d\":f%d -> \"node%d\";\n", node.blockId, fid, childBlockId) // for ptr edges
		db.printDotGraph(w, childBlockId)
	}
}

func (db *DB) printDebugInfo(blockId int) {
	node := loadNode(db, blockId)
	node.DebugInfo()
	if node.isLeaf() {
		return
	}
	i := 0
	for ; i <= node.nKeys(); i++ {
		childBlockId := node.getChildBlockId(i)
		db.printDebugInfo(childBlockId)
	}
}

func (db *DB) find(blockId int, key []byte) ([]byte, error) {
	node := loadNode(db, blockId)
	if node.isLeaf() {
		i, isEqual := node.findIndexInLeafByKey(key)
		if !isEqual {
			return nil, ERR_KEY_NOT_EXIST
		}
		val := node.getVal(i)
		return val, nil
	}
	i := node.findChildIndexByKey(key)
	childBlockId := node.getChildBlockId(i)
	return db.find(childBlockId, key)
}

func (db *DB) Delete(key []byte) {
	rootBlockId := int(*db.metaBlock.RootBlockId())
	rootNode := loadNode(db, rootBlockId)
	db.delete(rootNode, key)
	// 删除后，有可能root只剩下一个指针，没有key了，这时候需要去掉多余的root空节点
	if !rootNode.isLeaf() && rootNode.nKeys() == 0 {
		db.metaBlock.MutateRootBlockId(rootNode.ChildNodeId(0))
		db.blockMgr.recycleBlock(rootNode) // 回收旧的root node block
	}
}

func (node *Node) deleteInLeaf(key []byte) {
	i, isEqual := node.findIndexInLeafByKey(key)
	if !isEqual { // not found
		return
	}
	// assert key == node.keys[i], delete
	node.delKeyValByIndex(i)
	if node.nKeys() == 0 { // 完全delete空了，compact一下就没有垃圾了，并且重置了unused_mem_offset，hexdump好看一点
		node.compactMem()
	}
}

func (node *Node) delKeyValByIndex(i int) {
	node.clearKey(i) // note: 虽然似乎没用，因为后面的for循环会覆盖这个key指针，但这会正确更新actualMemRequired
	node.clearVal(i)
	for j := i + 1; j < node.nKeys(); j++ {
		node.setKeyPtr(j-1, node.getKeyPtr(j))
		node.setValPtr(j-1, node.getValPtr(j))
	}
	node.clearKeyPtr(node.nKeys() - 1)
	node.clearValPtr(node.nKeys() - 1)
	node.MutateNkeys(*node.Nkeys() - 1)
}

func (node *Node) removeMinKeyVal() (key, val []byte) {
	key = node.getKey(0)
	val = node.getVal(0)
	node.delKeyValByIndex(0)
	return
}

func (node *Node) removeMaxKeyVal() (key, val []byte) {
	nkeys := node.nKeys()
	key = node.getKey(nkeys - 1)
	val = node.getVal(nkeys - 1)
	node.delKeyValByIndex(nkeys - 1)
	return
}

func (node *Node) appendMaxKeyVal(key, val []byte) {
	node.insertKvInPos(node.nKeys(), key, val)
	node.setNKeys(node.nKeys() + 1)
}

func (node *Node) stealFromLeftLeaf(left *Node) (midKey []byte) {
	key, val := left.removeMaxKeyVal()
	node.insertKVInLeaf(key, val) // TODO: 是不是专门写个函数会好一点，不调用insertKv；
	return key                    // note: key is exactly the min key in curr
}

func (node *Node) stealFromRightLeaf(right *Node) (midKey []byte) {
	key, val := right.removeMinKeyVal()
	node.appendMaxKeyVal(key, val)
	return right.getLeftMostKey()
}

func (node *Node) mergeWithRightLeaf(right *Node) {
	//merge的时候，把在右边的key和val都copy过来，
	left := node
	j := left.nKeys()
	for i := 0; i < right.nKeys(); i++ {
		key := right.getKey(i)
		val := right.getVal(i)
		left.insertKvInPos(j, key, val) // actualMemRequired已经在insertKvInPos里更新了
		j++
	}
	left.setNKeys(left.nKeys() + right.nKeys())
	node.db.blockMgr.recycleBlock(right)
}

func (db *DB) delete(node *Node, key []byte) {
	if node.isLeaf() {
		node.deleteInLeaf(key)
		return
	}
	i := node.findChildIndexByKey(key)
	childBlockId := node.getChildBlockId(i)
	childNode := loadNode(db, childBlockId)
	db.delete(childNode, key)

	meta := db.metaBlock
	minKeys := int(*meta.Degree()) / 2
	if childNode.nKeys() < minKeys { // number of keys too small after deletion
		// try to steal from siblings
		// right sibling exists and can steal
		rightBlockId := node.getChildBlockId(i + 1)
		rightNode := loadNode(db, rightBlockId)
		if i+1 <= node.nKeys() && rightNode.nKeys() > minKeys {
			midKey := childNode.stealFromRight(rightNode, node.getKey(i))
			node.updateKey(i, midKey) // update the key
			return
		}
		// left sibling exists and can steal
		leftBlockId := node.getChildBlockId(i - 1)
		leftNode := loadNode(db, leftBlockId)
		if i-1 >= 0 && leftNode.nKeys() > minKeys {
			midKey := childNode.stealFromLeft(leftNode, node.getKey(i-1))
			node.updateKey(i-1, midKey)
			return
		}

		// if steal not possible, try to merge
		if i+1 <= node.nKeys() { // right sibling exists
			childNode.mergeWithRight(rightNode, node.getKey(i))
			//因为right sibling不应该存在了，所以parent对应的key和ptr也删除；
			node.delKeyandBlockIdByIndex(i, i+1)
			return
		}

		if i-1 >= 0 { // left sibling exists
			// 合并到左边的sibling去
			leftNode.mergeWithRight(childNode, node.getKey(i-1))
			node.delKeyandBlockIdByIndex(i-1, i)
			return
		}
	}
}

func (node *Node) stealFromLeft(left *Node, parentKey []byte) (midKey []byte) {
	if left.isLeaf() {
		return node.stealFromLeftLeaf(left)
	}
	// internal node
	/* 要拿到key来自于父节点，还要从left sibling那最大的一个指针过来，作为这边最小的指针，
	然后left sibling的最大的key变为父节点的key；
	*/
	key, ptr := left.removeMaxKeyAndBlockId()
	node.appendMinKeyAndBlockId(parentKey, ptr)
	return key
}

func (node *Node) stealFromRight(right *Node, parentKey []byte) (midKey []byte) {
	if right.isLeaf() {
		return node.stealFromRightLeaf(right)
	}
	// internal node
	/* 要拿到key来自于父节点，还要从right sibling那最小的一个指针过来，作为这边最大的指针，
	然后right sibling最小的key变为父节点的key；
	*/
	key, ptr := right.removeMinKeyAndBlockId()
	node.appendMaxKeyAndBlockId(parentKey, ptr)
	return key
}

func (node *Node) mergeWithRight(right *Node, parentKey []byte) {
	if node.isLeaf() {
		node.mergeWithRightLeaf(right)
		return
	}
	/* 和叶子节点的merge不同，需要先把parent的key copy过来，
	然后copy right sibling的key和ptr；然后删除parent的key和它相邻的右边的指针
	*/
	left := node
	leftNKeys := left.nKeys()
	left.insertKeyInPos(leftNKeys, parentKey)
	leftNKeys++
	j := leftNKeys
	for i := 0; i < right.nKeys(); i++ {
		left.insertKeyInPos(j, right.getKey(i))
		left.setChildBlockId(j, right.getChildBlockId(i))
		j++
	}
	left.setChildBlockId(j, right.getChildBlockId(right.nKeys()))
	left.setNKeys(leftNKeys + right.nKeys())

	node.db.blockMgr.recycleBlock(right)
}

func (node *Node) removeMinKeyAndBlockId() (key []byte, blockId int) {
	return node.delKeyandBlockIdByIndex(0, 0)
}

func (node *Node) appendMaxKeyAndBlockId(key []byte, blockId int) {
	nKeys := node.nKeys()
	node.insertKeyInPos(nKeys, key)
	node.setChildBlockId(nKeys+1, blockId)
	node.setNKeys(nKeys + 1)
}

func (node *Node) delKeyandBlockIdByIndex(keyStart, blockIdStart int) ([]byte, int) {
	deletedKey := node.getKey(keyStart)
	deletedBlockId := node.getChildBlockId(blockIdStart)
	node.clearKey(keyStart) // note: 虽然似乎没用，因为后面的for循环会覆盖这个，但这会正确更新actualMemRequired
	for j := keyStart + 1; j < node.nKeys(); j++ {
		node.setKeyPtr(j-1, node.getKeyPtr(j))
	}
	node.clearKeyPtr(node.nKeys() - 1)
	for j := blockIdStart + 1; j <= node.nKeys(); j++ {
		node.setChildBlockId(j-1, node.getChildBlockId(j))
	}
	node.setChildBlockId(node.nKeys(), 0)
	node.setNKeys(node.nKeys() - 1)
	return deletedKey, deletedBlockId
}

func (node *Node) removeMaxKeyAndBlockId() (key []byte, blockId int) {
	lastKeyIdx := node.nKeys() - 1
	lastBlockIdIdx := node.nKeys()
	return node.delKeyandBlockIdByIndex(lastKeyIdx, lastBlockIdIdx)
}

func (node *Node) appendMinKeyAndBlockId(key []byte, ptr int) {
	node.setNKeys(node.nKeys() + 1)
	for j := node.nKeys() - 1; j > 0; j-- {
		node.setKeyPtr(j, node.getKeyPtr(j-1))
	}
	for j := node.nKeys(); j > 0; j-- {
		node.setChildBlockId(j, node.getChildBlockId(j-1))
	}
	node.insertKeyInPos(0, key)
	node.setChildBlockId(0, ptr)
}

func (db *DB) insert(blockId int, key, val []byte) (promotedKey []byte, newSiblingNode *Node, err error) {
	node := loadNode(db, blockId)
	if node.isLeaf() {
		return node.insertKVInLeaf(key, val)
	}
	// internal node
	i := node.findChildIndexByKey(key)
	childBlockId := node.getChildBlockId(i)
	childPromtedKey, newChild, err := db.insert(childBlockId, key, val)
	if err != nil {
		return nil, nil, err
	}
	if newChild == nil { // no new child to insert
		return nil, nil, nil
	}
	// assert newChild != nil
	for l := node.nKeys() - 1; l >= i; l-- {
		node.setKeyPtr(l+1, node.getKeyPtr(l))
		node.setChildBlockId(l+2, node.getChildBlockId(l+1))
	}
	node.insertKeyInPos(i, childPromtedKey) // TODO: 因为key是通过key指针数组操作的，这里想要新分配一个key值，现在setKey还是insertKey语义有点不清晰，后续整理
	node.setChildBlockId(i+1, newChild.blockId)
	node.setNKeys(node.nKeys() + 1)

	if node.needSplit() {
		deg := node.degree()
		rightSibling := newInternalNode(db)
		nLeft := deg / 2
		nRight := deg - nLeft - 1
		l := node.nKeys() - 1

		for r := nRight - 1; r >= 0; r-- {
			rightSibling.insertKeyInPos(r, node.getKey(l))
			rightSibling.setChildBlockId(r+1, node.getChildBlockId(l+1))
			node.clearKey(l)
			node.setChildBlockId(l+1, 0)
			l--
		}
		rightSibling.setChildBlockId(0, node.getChildBlockId(l+1)) // 最左边的ptr
		node.setChildBlockId(l+1, 0)

		pKey := node.getKey(l)
		node.clearKey(l)
		node.setNKeys(nLeft)
		node.compactMem()
		rightSibling.setNKeys(nRight)
		return pKey, rightSibling, nil
	}
	return nil, nil, nil
}

func (node *Node) insertKVInLeaf(key, val []byte) (promotedKey []byte, newSiblingNode *Node, err error) {
	i, isEqual := node.findIndexInLeafByKey(key)
	if isEqual { // key already exists, update value only
		err := node.updateVal(i, val)
		return nil, nil, err
	}
	// i is the plact to insert
	for l := node.nKeys() - 1; l >= i; l-- {
		node.setKeyPtr(l+1, node.getKeyPtr(l))
		node.setValPtr(l+1, node.getValPtr(l))
	}
	node.insertKvInPos(i, key, val)
	node.setNKeys(node.nKeys() + 1)
	if node.needSplit() {
		db := node.db
		deg := node.degree()
		rightSibling := newLeafNode(db)
		nLeft := deg / 2
		nRight := deg - nLeft
		l := node.nKeys() - 1
		for r := nRight - 1; r >= 0; r-- {
			k := node.getKey(l)
			v := node.getVal(l)
			rightSibling.insertKvInPos(r, k, v)
			node.clearKey(l)
			node.clearVal(l)
			l--
		}
		node.setNKeys(nLeft)
		node.compactMem()
		rightSibling.setNKeys(nRight)
		return rightSibling.getKey(0), rightSibling, nil
	}
	return nil, nil, nil
}

func (db *DB) loadMetaBlock() {
	// TODO:
	start := BLOCK_MAGIC_SIZE // skip block start magic
	db.metaBlock = utils.GetRootAsMetaBlock(db.mmap[start:], 0)
	db.nUsedBlock = int(*db.metaBlock.NusedBlocks())
}

func (db *DB) Close() {
	db.mmap.Unmap()
	db.file.Close()
}
