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
    let k = key.dbValue
    let v = value.dbValue
    rocksdb_writebatch_put(batch, k, k.count, v, v.count)
  }

  public func put<K: DBSlice, V: DBSlice>(_ entries: [K: V]) {
    entries.forEach { put($0, value: $1) }
  }

  public func delete(_ key: DBSlice) {
    let k = key.dbValue
    rocksdb_writebatch_delete(batch, k, k.count)
  }

  public func delete<S: Sequence where S.Iterator.Element == DBSlice>(_ keys: S, options: DBWriteOptions? = nil) {
    keys.forEach { delete($0) }
  }
}
