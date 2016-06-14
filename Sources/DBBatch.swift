import CRocksDB

public class DBBatch {
  internal var batch: OpaquePointer

  public var count: Int {
    return Int(rocksdb_writebatch_count(batch))
  }

  init() {
    batch = rocksdb_writebatch_create()
  }

  deinit {
    rocksdb_writebatch_destroy(batch)
  }

  public func put(_ key: DBSlice, value: DBSlice) {
    rocksdb_writebatch_put(batch,
                           key.dbValue,
                           key.dbLength,
                           value.dbValue,
                           value.dbLength
    )
  }

  public func put<K: DBSlice, V: DBSlice>(_ entries: [K: V]) {
    entries.forEach { put($0, value: $1) }
  }

  public func delete(_ key: DBSlice) {
    rocksdb_writebatch_delete(batch, key.dbValue, key.dbLength)
  }

  public func delete<S: Sequence where S.Iterator.Element == DBSlice>(_ keys: S, options: DBWriteOptions? = nil) {
    keys.forEach { delete($0) }
  }
}
