/*
 * DBOptions.swift
 * Copyright (c) 2016 Ben Gollmer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import CRocksDB

public enum CompactionStyle: Int32 {
  case level      = 0
  case universal  = 1
}

public enum CompressionType: Int32 {
  case none   = 0
  case snappy = 1
  case zlib   = 2
  case bz2    = 3
  case lz4    = 4
  case lz4hc  = 5
}

public enum LogLevel: Int32 {
  case debug  = 0
  case info   = 1
  case warn   = 2
  case error  = 3
  case fatal  = 4
  case header = 5
}

public enum OptimizationStyle {
  case levelCompaction
  case levelCompactionWithBudget(Int)
  case pointLookup
  case pointLookupWithCacheSize(Int)
  case universalCompaction
  case universalCompactionWithBudget(Int)
}

public class DBOptions {
  internal var opts: OpaquePointer

  public var compactionStyle: CompactionStyle = .level {
    didSet {
      rocksdb_options_set_compaction_style(opts, compactionStyle.rawValue)
      switch compactionStyle {
      case .level:
        optimizeFor(.levelCompaction)
      case .universal:
        optimizeFor(.universalCompaction)
      }
    }
  }

  public var compression: CompressionType = .none {
    didSet {
      rocksdb_options_set_compression(opts, compression.rawValue)
    }
  }

  public var createIfMissing: Bool = true {
    didSet {
      rocksdb_options_set_create_if_missing(opts, createIfMissing ? 1 : 0)
    }
  }

  public var logLevel: LogLevel = .info {
    didSet {
      rocksdb_options_set_info_log_level(opts, logLevel.rawValue)
    }
  }

  public var parallelism: Int = 0 {
    didSet {
      let p = parallelism == 0 ? sysconf(Int32(_SC_NPROCESSORS_ONLN)) : parallelism
      rocksdb_options_increase_parallelism(opts, Int32(p))
    }
  }

  public init() {
    opts = rocksdb_options_create()

    /* Property observers are not called during init */
    rocksdb_options_set_compaction_style(opts, CompactionStyle.level.rawValue)
    optimizeFor(.levelCompaction)
    rocksdb_options_set_create_if_missing(opts, 1)
    rocksdb_options_increase_parallelism(opts, Int32(sysconf(Int32(_SC_NPROCESSORS_ONLN))))
  }

  deinit {
    rocksdb_options_destroy(opts)
  }

  public func enableStatistics() {
    rocksdb_options_enable_statistics(opts)
  }

  public func getStatistics() -> String? {
    let s = rocksdb_options_statistics_get_string(opts)
    guard let stats = s else { return nil }
    defer { rocksdb_free(s) }
    return String(cString: stats)
  }

  public func optimizeFor(_ style: OptimizationStyle) {
    switch style {
    case .levelCompaction:
      rocksdb_options_optimize_level_style_compaction(opts, 0)
    case .levelCompactionWithBudget(let budget):
      rocksdb_options_optimize_level_style_compaction(opts, UInt64(budget))
    case .pointLookup:
      rocksdb_options_optimize_for_point_lookup(opts, 0)
    case .pointLookupWithCacheSize(let cacheSize):
      rocksdb_options_optimize_for_point_lookup(opts, UInt64(cacheSize))
    case .universalCompaction:
      rocksdb_options_optimize_universal_style_compaction(opts, 0)
    case .universalCompactionWithBudget(let budget):
      rocksdb_options_optimize_universal_style_compaction(opts, UInt64(budget))
    }
  }
}

public enum ReadTier: Int32 {
  case readAll    = 0
  case blockCache = 1
  case persisted  = 2
}

public class DBReadOptions {
  internal var opts = rocksdb_readoptions_create()

  public var fillCache: Bool = true {
    didSet {
      rocksdb_readoptions_set_fill_cache(opts, fillCache ? 1 : 0)
    }
  }

  public var readaheadSize: Int = 0 {
    didSet {
      rocksdb_readoptions_set_readahead_size(opts, readaheadSize)
    }
  }

  public var readSnapshot: DBReadSnapshot? = nil {
    didSet {
      guard let snap = readSnapshot else { return }
      rocksdb_readoptions_set_snapshot(opts, snap.snapshot)
    }
  }

  public var readTier: ReadTier = .readAll {
    didSet {
      rocksdb_readoptions_set_read_tier(opts, readTier.rawValue)
    }
  }

  public var verifyChecksums: Bool = false {
    didSet {
      rocksdb_readoptions_set_verify_checksums(opts, verifyChecksums ? 1 : 0)
    }
  }

  deinit {
    rocksdb_readoptions_destroy(opts)
  }
}

public class DBWriteOptions {
  internal var opts = rocksdb_writeoptions_create()

  public var disableWAL: Bool = false {
    didSet {
      rocksdb_writeoptions_disable_WAL(opts, disableWAL ? 1 : 0)
    }
  }

  public var sync: Bool = false {
    didSet {
      rocksdb_writeoptions_set_sync(opts, sync ? 1 : 0)
    }
  }

  deinit {
    rocksdb_writeoptions_destroy(opts)
  }
}
