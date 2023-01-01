package btreedb

import (
	"btreedb/utils"

	"github.com/edsrzf/mmap-go"
	flatbuffers "github.com/google/flatbuffers/go"
)

type BlockMgr struct {
	metaBlock *utils.MetaBlock
	mmap      mmap.MMap
}

func NewBlockMgr(mmap mmap.MMap) *BlockMgr {
	return &BlockMgr{
		mmap:      mmap,
	}
}

/*
meta block的格式:
8个字节的block start Magic，仅用来辅助其他工具查看DB文件，程序中没多大用
剩下的在flatbuffers MetaBlock table里
*/
func (mgr *BlockMgr) buildMetaBlock(degree int) *utils.MetaBlock{
	_, start := mgr.newBlockZero() // for metablock, block 0
	rootBlockId := 1          // root node id 为1，虽然还没有构造

	builder := flatbuffers.NewBuilder(1024)
	utils.MetaBlockStart(builder)
	utils.MetaBlockAddNusedBlocks(builder, 1)	// 目前只用了1个block
	utils.MetaBlockAddNkeys(builder, 0)
	utils.MetaBlockAddRootBlockId(builder, uint32(rootBlockId))
	utils.MetaBlockAddDegree(builder, uint32(degree))
	builder.Finish(utils.MetaBlockEnd(builder))
	buf := builder.FinishedBytes()
	copy(mgr.mmap[start:], buf) // 把构造好的metablock数据copy到mmap内存中

	metaBlock := utils.GetRootAsMetaBlock(mgr.mmap[start:], 0)
	return metaBlock
}

func (mgr *BlockMgr) newBlock() (uint32, Offset) {
	blockId, ok := mgr.popRecycledBlock()
	if !ok {
		blockId = *mgr.metaBlock.NusedBlocks()
	}
	mgr.metaBlock.MutateNusedBlocks(blockId + 1)
	return mgr.allocBlock(blockId)
}

// meta block是特殊分配，先分配才能构造metaBlock结构
func (mgr *BlockMgr) newBlockZero() (uint32, Offset) {
	blockId := uint32(0)
	return mgr.allocBlock(blockId)
}


func (mgr *BlockMgr) setMetaBlock(metaBlock *utils.MetaBlock) {
	mgr.metaBlock = metaBlock
}


func (mgr *BlockMgr) allocBlock(blockId uint32) (uint32, Offset) {
	var start Offset = Offset(blockId) * BLOCK_SIZE
	copy(mgr.mmap[start:], mmap.MMap(BLOCK_MAGIC))
	start += Offset(BLOCK_MAGIC_SIZE)
	return blockId, start
}

func (mgr *BlockMgr) popRecycledBlock() (blockId uint32, ok bool) {
	// TODO
	return 0, false
}

func (mgr *BlockMgr) recycleBlock(blockId uint32) {

}
