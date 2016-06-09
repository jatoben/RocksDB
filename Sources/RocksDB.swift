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
    return String(cString: val)
  }
}
