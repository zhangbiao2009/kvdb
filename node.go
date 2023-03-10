package btreedb

import (
	"btreedb/utils"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/edsrzf/mmap-go"
	flatbuffers "github.com/google/flatbuffers/go"
)

type Node struct {
	blockId   int
	metaBlock *utils.MetaBlock
	mmap      mmap.MMap
	*utils.NodeBlock
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

// 在i处新增一个child
func (node *Node) insertNewChild(i int, childPromtedKey []byte, newChild *Node) {
	// assert newChild != nil
	for l := node.nKeys() - 1; l >= i; l-- {
		node.setKeyPtr(l+1, node.getKeyPtr(l))
		node.setChildBlockId(l+2, node.getChildBlockId(l+1))
	}
	node.insertKeyInPos(i, childPromtedKey)
	node.setChildBlockId(i+1, newChild.blockId)
	node.setNKeys(node.nKeys() + 1)
}

func (node *Node) appendMinKeyAndBlockId(key []byte, ptr int) { // TODO: 跟insertNewChild的逻辑有些重复，是否能合并
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

func (node *Node) getLeftMostKey() []byte {
	return node.getKey(0)
}

// 在叶子节点上，找key在的或者应该在的index，如果是相等的情况isEqual为true
func (node *Node) findIndexInLeafByKey(key []byte) (index int, isEqual bool) {
	i := 0
	for ; i < node.nKeys(); i++ {
		k := node.getKey(i)
		cmp := bytes.Compare(key, k)
		if cmp == 0 { // found
			return i, true
		}
		if cmp < 0 {
			break
		}
	}
	return i, false
}

// 在中间节点上操作，返回key应该在的child的index
func (node *Node) findChildIndexByKey(key []byte) int {
	i := 0
	for ; i < node.nKeys(); i++ {
		k := node.getKey(i)
		cmp := bytes.Compare(key, k)
		if cmp < 0 {
			break
		}
	}
	return i
}

func (node *Node) insertKvInPos(i int, key, val []byte) error {
	sizeKey := calcRequiredMemSerialized(key)
	sizeVal := calcRequiredMemSerialized(val)
	if err := node.spaceIsEnough(int(sizeKey + sizeVal)); err != nil {
		return err
	}
	node.insertKeyInPos(i, key)
	_, err := node.insertValInPos(i, val) // TODO: 前面检查内存够不够了，这里是不是可以不检查了
	return err
}

// TODO: insertKeyInPos一定能成功，所以需要根据最大key size预留足够的空间
func (node *Node) insertKeyInPos(i int, key []byte) uint16 {
	sizeKey := calcRequiredMemSerialized(key)
	if err := node.spaceIsEnough(int(sizeKey)); err != nil { // TODO: 调用spaceIsEnough是为了触发可能需要compact，但现在看起来意图不明显，需要改进
		panic(err)
	}
	newKeyOffset, _ := node.appendToFreeMem(key)
	node.setKeyPtr(i, newKeyOffset)
	actualMem := *node.ActualMemRequired()
	node.MutateActualMemRequired(actualMem + sizeKey)
	return sizeKey
}

func (node *Node) insertValInPos(i int, val []byte) (uint16, error) {
	sizeVal := calcRequiredMemSerialized(val)
	err := node.spaceIsEnough(int(sizeVal))
	if err != nil {
		return 0, err
	}
	newValOffset, _ := node.appendToFreeMem(val)
	node.setValPtr(i, newValOffset)

	actualMem := *node.ActualMemRequired()
	node.MutateActualMemRequired(actualMem + sizeVal)
	return uint16(sizeVal), nil
}

func (node *Node) appendToFreeMem(content []byte) (contentOffset int, contentSize uint16) {
	ununsedOffset := int(*node.UnusedMemOffset())
	freeMemOffset := ununsedOffset + node.blockId*BLOCK_SIZE
	size := putByteSlice(node.mmap[freeMemOffset:], content)
	node.MutateUnusedMemOffset(uint16(ununsedOffset + size))
	return freeMemOffset, uint16(size) // TODO: 如果允许超大的value(跨越多个block的)， uint16也许不够表示value的size
}

/*
检查剩余空间是否能容纳valSize大小的数据，如果可以直接返回；如果不行先计算compact后能不能容纳下，
如果可以compact后返回，如果不行返回error
暂时先这么做，后续策略可允许value有overflow blocks；
*/
func (node *Node) spaceIsEnough(valSize int) error {
	ununsedOffset := int(*node.UnusedMemOffset())
	if BLOCK_SIZE-ununsedOffset >= valSize { // 空间够
		return nil
	}

	actualMem := *node.ActualMemRequired()
	memAvailable := BLOCK_SIZE - *node.UnusedMemStart() - actualMem

	if memAvailable >= uint16(valSize) { // 压缩后空间够
		node.compactMem()
		return nil
	}

	err := errors.New("not enough memory in this block")
	return err // 压缩后也不够
}

func (node *Node) compactMem() {
	unusedMemStart := int(*node.UnusedMemStart())
	tmpMem := make([]byte, *node.UnusedMemOffset()-uint16(unusedMemStart)) // 大小正好覆盖所有写过的空间
	start := 0
	for i := 0; i < node.nKeys(); i++ {
		key := node.getKey(i)
		size := putByteSlice(tmpMem[start:], key)
		node.MutateKeyPtrArr(i, uint16(unusedMemStart+start)) // 更新key的偏移量
		start += size
		if node.isLeaf() {
			val := node.getVal(i)
			size = putByteSlice(tmpMem[start:], val) // 更新val的偏移量
			node.MutateValPtrArr(i, uint16(unusedMemStart+start))
			start += size
		}
	}
	// copy compact之后的内容，顺带清空没用的空间
	copy(node.mmap[node.blockId*BLOCK_SIZE+unusedMemStart:], tmpMem)
	node.MutateUnusedMemOffset(uint16(unusedMemStart + start))
}

func (node *Node) clearKey(i int) {
	sizeKey := node.getKeySize(i)
	node.clearKeyPtr(i)
	actualMem := *node.ActualMemRequired()
	node.MutateActualMemRequired(actualMem - sizeKey)
}

func (node *Node) updateKey(i int, key []byte) { // update key i
	// 为简化实现，直接append，原先空间作废
	sizeOldKey := node.getKeySize(i)
	node.insertKeyInPos(i, key)

	actualMem := *node.ActualMemRequired()
	node.MutateActualMemRequired(actualMem - sizeOldKey)
}

func (node *Node) clearVal(i int) {
	sizeVal := node.getValSize(i)
	node.clearValPtr(i)
	actualMem := *node.ActualMemRequired()
	node.MutateActualMemRequired(actualMem - sizeVal)
}

func (node *Node) updateVal(i int, val []byte) error { // update val i
	// 可以先看val i原先所占的大小够不够，如果够就原地修改，如果不够就新分配空间写入，原来的val i所占的空间变为garbage
	// 为简化实现，直接append，原先空间作废
	sizeOldVal := node.getValSize(i)
	_, err := node.insertValInPos(i, val)
	if err != nil {
		return err
	}
	actualMem := *node.ActualMemRequired()
	node.MutateActualMemRequired(actualMem - sizeOldVal)
	return nil
}

func (node *Node) isLeaf() bool {
	return *node.IsLeaf()
}

func (node *Node) degree() int {
	return int(*node.metaBlock.Degree())
}
func (node *Node) nKeys() int {
	res := int(*node.NodeBlock.Nkeys())
	return res
}

func (node *Node) needSplit() bool {
	return node.nKeys() == node.degree()-1
}

func (node *Node) setNKeys(nKeys int) {
	node.MutateNkeys(uint16(nKeys))
}

func (node *Node) getKey(i int) []byte {
	keyOffset := node.getKeyPtr(i)
	return getByteSlice(node.mmap[keyOffset:])
}

func (node *Node) getKeySize(i int) uint16 { // 返回key在block内存中占用的空间大小
	keyOffset := node.getKeyPtr(i)
	sz := getByteSliceSize(node.mmap[keyOffset:])
	return uint16(sz)
}

func (node *Node) getVal(i int) []byte {
	valOffset := node.getValPtr(i)
	return getByteSlice(node.mmap[valOffset:])
}

func (node *Node) getValSize(i int) uint16 { // 返回val在block内存中占用的空间大小
	valOffset := node.getValPtr(i)
	sz := getByteSliceSize(node.mmap[valOffset:])
	return uint16(sz)
}

func (node *Node) getKeyPtr(i int) int {
	return int(node.KeyPtrArr(i)) + node.blockId*BLOCK_SIZE
}

func (node *Node) setKeyPtr(i int, val int) {
	node.MutateKeyPtrArr(i, uint16(val-node.blockId*BLOCK_SIZE)) // 存储相对于本block开始的偏移量
}
func (node *Node) clearKeyPtr(i int) {
	node.setKeyPtr(i, node.blockId*BLOCK_SIZE) // 这样达到最终keyPtrArr[i] = 0的效果
}

func (node *Node) getValPtr(i int) int {
	return int(node.ValPtrArr(i)) + node.blockId*BLOCK_SIZE
}

func (node *Node) setValPtr(i int, val int) {
	node.MutateValPtrArr(i, uint16(val-node.blockId*BLOCK_SIZE)) // 存储相对于本block开始的偏移量
}

func (node *Node) clearValPtr(i int) {
	node.setValPtr(i, node.blockId*BLOCK_SIZE)
}

func (node *Node) getChildBlockId(i int) int {
	return int(node.ChildNodeId(i))
}

func (node *Node) setChildBlockId(i int, blockId int) {
	node.MutateChildNodeId(i, uint32(blockId))
}

func (node *Node) DebugInfo() {
	fmt.Fprintf(os.Stderr, "node block id: %d\n", node.blockId)
	fmt.Fprintf(os.Stderr, "\t isLeaf: %t\n", node.isLeaf())
	fmt.Fprintf(os.Stderr, "\t nKeys: %d\n", node.nKeys())
	fmt.Fprintf(os.Stderr, "\t unused_mem_start: %d\n", *node.UnusedMemStart())
	fmt.Fprintf(os.Stderr, "\t unused_mem_offset: %d\n", *node.UnusedMemOffset())
	actualMemRequred := *node.ActualMemRequired()
	fmt.Fprintf(os.Stderr, "\t actual_mem_required: %d\n", actualMemRequred)
	expectActualMemRequred := uint16(0)
	fmt.Fprintf(os.Stderr, "\t keys: ")
	for i := 0; i < node.nKeys(); i++ {
		expectActualMemRequred += node.getKeySize(i)
		k := node.getKey(i)
		fmt.Fprintf(os.Stderr, "%s ", string(k))
	}
	fmt.Println()
	if node.isLeaf() {
		fmt.Fprintf(os.Stderr, "\t vals: ")
		for i := 0; i < node.nKeys(); i++ {
			expectActualMemRequred += node.getValSize(i)
			v := node.getVal(i)
			fmt.Fprintf(os.Stderr, "%s ", string(v))
		}
		fmt.Println()
	} else {
		fmt.Fprintf(os.Stderr, "\t child block ids: ")
		for i := 0; i <= node.nKeys(); i++ {
			fmt.Fprintf(os.Stderr, "%d ", node.getChildBlockId(i))
		}
		fmt.Println()
	}
	if expectActualMemRequred != actualMemRequred {
		fmt.Fprintf(os.Stderr, "error not equal: actualMemRequred: %d, expectActualMemRequred: %d\n", actualMemRequred, expectActualMemRequred)
	}
	fmt.Println()
}

func (node *Node) clearNodeContent() (blockId int, start int) {
	blockId = node.blockId
	start = blockId*BLOCK_SIZE + BLOCK_MAGIC_SIZE // TODO：这个东西出现的地方太多了，最好聚集到一个地方
	unusedOffset := node.UnusedMemOffset()
	end := blockId*BLOCK_SIZE + int(*unusedOffset)
	mmap := node.mmap
	for i := start; i < end; i++ {
		mmap[i] = 0
	}
	return
}

func (node *Node) printDotGraphLeaf(w io.Writer) {
	fmt.Fprintf(w, "node%d [label = \"", node.blockId)
	fmt.Fprintf(w, "<f0> ") // for prev ptr
	for i := 0; i < node.nKeys(); i++ {
		fmt.Fprintf(w, "|<f%d> %v", 2*i+1, string(node.getKey(i))) // for the keys[i]
		//fmt.Fprintf(w, "|<f%d> val:%d", 2*i+1, node.vals[i])	// for the vals[i]
		fmt.Fprintf(w, "|<f%d> v", 2*i+2) // for the vals[i]
	}
	nextPtrFid := 2*node.nKeys() + 1
	fmt.Fprintf(w, "|<f%d> ", nextPtrFid) // for next ptr
	fmt.Fprintln(w, "\"];")
	// 打印这两个指针会导致图形layout不好看，没找到好办法前暂时不打印
	/*
		if node.prev != nil {
			fid := 2*node.prev.nkeys + 1
			fmt.Fprintf(w, "\"node%d\":f0 -> \"node%d\":f%d;\n", node.id, node.prev.getNodeId(), fid) // for prev ptr edge
		}
		if node.next != nil {
			fmt.Fprintf(w, "\"node%d\":f%d -> \"node%d\":f0;\n", node.id, nextPtrFid, node.next.getNodeId()) // for next ptr edge
		}
	*/
}

func newNodeBlock(outputBuf []byte, degree int, isLeaf bool) *utils.NodeBlock {
	builder := flatbuffers.NewBuilder(1024)

	var childBlockIdArr flatbuffers.UOffsetT
	var valPtrArr flatbuffers.UOffsetT
	if isLeaf {
		vecSize := degree - 1
		utils.NodeBlockStartValPtrArrVector(builder, vecSize)
		for i := vecSize - 1; i >= 0; i-- {
			builder.PrependUint16(0)
		}
		valPtrArr = builder.EndVector(vecSize)
	} else {
		vecSize := degree
		utils.NodeBlockStartChildNodeIdVector(builder, vecSize)
		for i := vecSize - 1; i >= 0; i-- {
			builder.PrependUint32(0)
		}
		childBlockIdArr = builder.EndVector(vecSize)
	}
	vecSize := degree - 1
	utils.NodeBlockStartKeyPtrArrVector(builder, vecSize)
	for i := vecSize - 1; i >= 0; i-- {
		builder.PrependUint16(0)
	}
	keyPtrArr := builder.EndVector(vecSize)

	utils.NodeBlockStart(builder)
	utils.NodeBlockAddIsLeaf(builder, isLeaf)
	utils.NodeBlockAddPadding(builder, 0)
	utils.NodeBlockAddNkeys(builder, 0)
	utils.NodeBlockAddKeyPtrArr(builder, keyPtrArr)
	if isLeaf {
		utils.NodeBlockAddValPtrArr(builder, valPtrArr)
	} else {
		utils.NodeBlockAddChildNodeId(builder, childBlockIdArr)
	}
	utils.NodeBlockAddActualMemRequired(builder, 0)
	utils.NodeBlockAddUnusedMemStart(builder, 0)
	utils.NodeBlockAddUnusedMemOffset(builder, 0)
	builder.Finish(utils.NodeBlockEnd(builder))
	bytes := builder.FinishedBytes()
	nBytesUsed := uint16(len(bytes))

	copy(outputBuf, bytes) // 构造好后，copy到mmap中

	nodeBlock := utils.GetRootAsNodeBlock(outputBuf, 0)
	unusedMemOffset := uint16(BLOCK_MAGIC_SIZE) + nBytesUsed
	nodeBlock.MutateUnusedMemStart(unusedMemOffset)
	nodeBlock.MutateUnusedMemOffset(unusedMemOffset) // set unused mem offset
	return nodeBlock
}
