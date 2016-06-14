import CRocksDB

enum LogLevel: Int {
  case debug  = 0
  case info   = 1
  case warn   = 2
  case error  = 3
  case fatal  = 4
  case header = 5
}

public class DBOptions {
  private var opts: OpaquePointer = rocksdb_options_create()
  static let ParallelismAuto = -1

  var parallelism: Int = ParallelismAuto
  var createIfMissing: Bool = true
  var optimizeLevelStyleCompaction: Bool = true
  var enableStatistics: Bool = false
  var logLevel: LogLevel? = nil

  internal func options() -> OpaquePointer {
    rocksdb_options_set_create_if_missing(opts, createIfMissing ? 1 : 0)
    let p = (self.parallelism == DBOptions.ParallelismAuto) ?
      Int(sysconf(_SC_NPROCESSORS_ONLN)) : self.parallelism
    rocksdb_options_increase_parallelism(opts, Int32(p))
    if self.optimizeLevelStyleCompaction {
      rocksdb_options_optimize_level_style_compaction(opts, 0);
    }
    if let level = logLevel {
      rocksdb_options_set_info_log_level(opts, Int32(level.rawValue))
    }
    if enableStatistics {
      rocksdb_options_enable_statistics(opts)
    }

    return opts
  }

  deinit {
    rocksdb_options_destroy(opts)
  }

  public func getStatistics() -> String? {
    let s = rocksdb_options_statistics_get_string(opts)
    guard let stats = s else { return nil }
    defer { free(s) }
    return String(cString: stats)
  }
}

public class DBReadOptions {
  internal var opts = rocksdb_readoptions_create()

  deinit {
    rocksdb_readoptions_destroy(opts)
  }

  func setReadSnapshot(_ snapshot: DBReadSnapshot) {
    rocksdb_readoptions_set_snapshot(opts, snapshot.snapshot)
  }
}

public class DBWriteOptions {
  internal var opts = rocksdb_writeoptions_create()

  deinit {
    rocksdb_writeoptions_destroy(opts)
  }
}
