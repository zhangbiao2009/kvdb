# kvdb
kvdb is a disk-based key value DB, which uses B+tree to enable fast insert, delete and find operations.
FlatBuffers is used to operate directly on memory, avoiding serialization and deserialization cost like ProtoBuffers.
