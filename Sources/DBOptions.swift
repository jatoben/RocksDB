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
