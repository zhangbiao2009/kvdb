package btreedb

import "encoding/binary"

func putByteSlice(b, s []byte) int {
	slen := uint64(len(s))
	nbytes := binary.PutUvarint(b, slen)
	copy(b[nbytes:], s)
	return nbytes + len(s)
}

// 从序列化好的内存中读取一个byte slice出来
func getByteSlice(b []byte) []byte {
	slen, nbytes := binary.Uvarint(b)
	resSlice := make([]byte, slen)
	copy(resSlice[:slen], b[nbytes:nbytes+int(slen)])
	return resSlice
}

func getByteSliceSize(b []byte) int { // 获取当前存储在内存中的byte slice占用的空间的大小
	slen, nbytes := binary.Uvarint(b)
	return nbytes + int(slen)
}

func calcRequiredMemSerialized(s []byte) uint16 { // 计算s序列化后需要占用的空间的大小
	b := make([]byte, 10) // varint 最多需要10个字节
	slen := len(s)
	nbytes := binary.PutUvarint(b, uint64(slen))
	return uint16(nbytes + slen)
}

func putUint32(b []byte, v uint32) {
	binary.LittleEndian.PutUint32(b, v)
}

func getUint32(b []byte) uint32 {
	return binary.LittleEndian.Uint32(b)
}
