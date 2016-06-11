import CRocksDB

public class DBOptions {
  private var opts: OpaquePointer = rocksdb_options_create()
  static let ParallelismAuto = -1

  var parallelism: Int = ParallelismAuto
  var createIfMissing: Bool = true
  var optimizeLevelStyleCompaction: Bool = true

  internal func options() -> OpaquePointer {
    rocksdb_options_set_create_if_missing(opts, createIfMissing ? 1 : 0)
    let p = (self.parallelism == DBOptions.ParallelismAuto) ?
      Int(sysconf(_SC_NPROCESSORS_ONLN)) : self.parallelism
    rocksdb_options_increase_parallelism(opts, Int32(p))
    if self.optimizeLevelStyleCompaction {
      rocksdb_options_optimize_level_style_compaction(opts, 0);
    }

    return opts
  }

  deinit {
    rocksdb_options_destroy(opts)
  }
}

public class DBReadOptions {
  internal var opts = rocksdb_readoptions_create()

  deinit {
    rocksdb_readoptions_destroy(opts)
  }
}

public class DBWriteOptions {
  internal var opts = rocksdb_writeoptions_create()

  deinit {
    rocksdb_writeoptions_destroy(opts)
  }
}

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

  func put(_ key: String, value: String) {
    rocksdb_writebatch_put(batch,
      key,
      key.utf8.count,
      value,
      value.utf8.count
    )
  }
}

private extension String {
  init(UTF8Bytes bytes: UnsafePointer<Int8>, length: Int) {
    var buf = [Int8](repeating: 0, count: length + 1)
    for i in 0..<length { buf[i] = bytes[i] }
    self.init(cString: buf)
  }
}

public class DBIterator: IteratorProtocol {
  private var iter: OpaquePointer

  internal init(_ iter: OpaquePointer, _ keyPrefix: String? = nil) {
    self.iter = iter

    if let prefix = keyPrefix {
      rocksdb_iter_seek(iter, prefix, prefix.utf8.count)
    } else {
      rocksdb_iter_seek_to_first(self.iter)
    }
  }

  deinit {
    rocksdb_iter_destroy(iter)
  }

  public func next() -> (String, String)? {
    var keyLength: Int = 0
    var valLength: Int = 0

    guard rocksdb_iter_valid(iter) != 0 else { return nil }

    let k = rocksdb_iter_key(iter, &keyLength)
    let v = rocksdb_iter_value(iter, &valLength)
    guard let key = k, let val = v else { return nil }

    defer { rocksdb_iter_next(iter) }
    return (String(UTF8Bytes: key, length: keyLength), String(UTF8Bytes: val, length: valLength))
  }
}

extension Database: Sequence {
  public func makeIterator(_ opts: DBReadOptions, keyPrefix prefix: String? = nil) -> DBIterator {
    let i = rocksdb_create_iterator(db, opts.opts)
    guard let iter = i else { preconditionFailure("Could not create database iterator") }
    return DBIterator(iter, prefix)
  }

  public func makeIterator(keyPrefix prefix: String) -> DBIterator {
    return makeIterator(defaultReadOptions, keyPrefix: prefix)
  }

  public func makeIterator() -> DBIterator {
    return makeIterator(defaultReadOptions)
  }
}

public class Database {
  private var db: OpaquePointer
  private lazy var defaultReadOptions = { DBReadOptions() }()
  private lazy var defaultWriteOptions = { DBWriteOptions() }()

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

  func put(_ key: String, value: String, options: DBWriteOptions? = nil) throws {
    let opts = options ?? defaultWriteOptions
    var err: UnsafeMutablePointer<Int8>? = nil
    rocksdb_put(db,
      opts.opts,
      key,
      key.utf8.count,
      value,
      value.utf8.count,
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

  func delete(_ key: String, options: DBWriteOptions? = nil) throws {
    let opts = options ?? defaultWriteOptions
    var err: UnsafeMutablePointer<Int8>? = nil
    rocksdb_delete(db, opts.opts, key, key.utf8.count, &err)

    guard err == nil else {
      defer { free(err) }
      throw DBError.WriteFailed(String(cString: err!))
    }
  }

  func get(_ key: String, options: DBReadOptions? = nil) throws -> String? {
    let opts = options ?? defaultReadOptions
    var err: UnsafeMutablePointer<Int8>? = nil
    var valueLength: Int = 0
    let value = rocksdb_get(db,
      opts.opts,
      key,
      key.utf8.count,
      &valueLength,
      &err
    )

    guard err == nil else {
      defer { free(err) }
      throw DBError.PutFailed(String(cString: err!))
    }

    guard let val = value else { return nil }
    defer { free(val) }
    return String(UTF8Bytes: val, length: valueLength)
  }
}
