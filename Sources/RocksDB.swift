import CRocksDB

public enum DBError: ErrorProtocol, CustomStringConvertible {
  case OpenFailed(String)
  case GetFailed(String)
  case PutFailed(String)
  case WriteFailed(String)

  public var description: String {
    switch self {
    case let OpenFailed(s):
      return "Open failed: \(s)"
    case let GetFailed(s):
      return "Get failed: \(s)"
    case let PutFailed(s):
      return "Put failed: \(s)"
    case let WriteFailed(s):
      return "Write failed: \(s)"
    }
  }
}

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

  func put(_ key: DBSlice, value: DBSlice) {
    rocksdb_writebatch_put(batch,
      key.dbValue,
      key.dbLength,
      value.dbValue,
      value.dbLength
    )
  }

  func put<K: DBSlice, V: DBSlice>(_ entries: [K: V]) {
    entries.forEach { put($0, value: $1) }
  }

  func delete(_ key: DBSlice) {
    rocksdb_writebatch_delete(batch, key.dbValue, key.dbLength)
  }

  func delete<S: Sequence where S.Iterator.Element == DBSlice>(_ keys: S, options: DBWriteOptions? = nil) {
    keys.forEach { delete($0) }
  }
}

public class Database {
  internal var db: OpaquePointer
  internal lazy var defaultReadOptions = DBReadOptions()
  internal lazy var defaultWriteOptions = DBWriteOptions()

  init(path: String, options: DBOptions? = nil) throws {
    let o = options ?? DBOptions()
    var err: UnsafeMutablePointer<Int8>? = nil
    db = rocksdb_open(o.options(), path, &err)

    guard err == nil else {
      defer { free(err) }
      throw DBError.OpenFailed(String(cString: err!))
    }
  }

  deinit {
    rocksdb_close(db)
  }

  func put<K: DBSlice, V: DBSlice>(_ key: K, value: V, options: DBWriteOptions? = nil) throws {
    let opts = options ?? defaultWriteOptions
    var err: UnsafeMutablePointer<Int8>? = nil
    rocksdb_put(db,
      opts.opts,
      key.dbValue,
      key.dbLength,
      value.dbValue,
      value.dbLength,
      &err
    )

    guard err == nil else {
      defer { free(err) }
      throw DBError.PutFailed(String(cString: err!))
    }
  }

  func write(_ batch: DBBatch, options: DBWriteOptions? = nil) throws {
    let opts = options ?? defaultWriteOptions
    var err: UnsafeMutablePointer<Int8>? = nil
    rocksdb_write(db,
      opts.opts,
      batch.batch,
      &err
    )

    guard err == nil else {
      defer { free(err) }
      throw DBError.WriteFailed(String(cString: err!))
    }
  }

  func delete<K: DBSlice>(_ key: K, options: DBWriteOptions? = nil) throws {
    let opts = options ?? defaultWriteOptions
    var err: UnsafeMutablePointer<Int8>? = nil
    rocksdb_delete(db, opts.opts, key.dbValue, key.dbLength, &err)

    guard err == nil else {
      defer { free(err) }
      throw DBError.WriteFailed(String(cString: err!))
    }
  }

  func get<K: DBSlice, V: DBSlice>(_ key: K, options: DBReadOptions? = nil) throws -> V? {
    let opts = options ?? defaultReadOptions
    var err: UnsafeMutablePointer<Int8>? = nil
    var valLength: Int = 0
    let value = rocksdb_get(db,
      opts.opts,
      key.dbValue,
      key.dbLength,
      &valLength,
      &err
    )

    guard err == nil else {
      defer { free(err) }
      throw DBError.PutFailed(String(cString: err!))
    }

    guard let val = value else { return nil }
    defer { free(val) }
    let valPointer = UnsafeBufferPointer(start: val, count: valLength)
    return V(dbValue: [Int8](valPointer))
  }
}
