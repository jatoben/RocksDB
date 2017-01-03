/*
 * DBProperty.swift
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

/**
 * The variables in this struct identify specific database properties that
 * can be retrieved with `Database.getProperty()`.
 *
 * This list may not be completely comprehensive, depending on the version
 * of RocksDB that you're using. See the `rocksdb/db.h` header for a full
 * list.
 */
public struct DBProperty: RawRepresentable {
  public typealias RawValue = String
  public var rawValue: RawValue

  /** Returns statistics about column families in the database. */
  public static let columnFamilyStats = DBProperty(rawValue: "rocksdb.cfstats")

  /** Returns general database statistics. */
  public static let databaseStats = DBProperty(rawValue: "rocksdb.dbstats")

  /** Retrieves both column family statistics and general database statistics. */
  public static let stats = DBProperty(rawValue: "rocksdb.stats")

  /** Describes the SST files used by the database. */
  public static let sstables = DBProperty(rawValue: "rocksdb.sstables")

  /** Describes the number of files per level and the size of each level in MB. */
  public static let levelStats = DBProperty(rawValue: "rocksdb.levelstats")

  /** The number of immutable memtables that have not yet been flushed. */
  public static let immutableMemtables = DBProperty(rawValue: "rocksdb.num-immutable-mem-table")

  /** The number of imutable memtables that have been flushed. */
  public static let immutableMemtablesFlushed = DBProperty(rawValue: "rocksdb.num-immutable-mem-table-flushed")

  /** Returns 1 if a memtable flush is pending, 0 otherwise. */
  public static let memtableFlushPending = DBProperty(rawValue: "rocksdb.mem-table-flush-pending")

  /** The number of currently running flush operations. */
  public static let runningFlushes = DBProperty(rawValue: "rocksdb.num-running-flushes")

  /** Returns 1 if a compaction is pending, 0 otherwise. */
  public static let compactionPending = DBProperty(rawValue: "rocksdb.compaction-pending")

  /** The number of currently running compaction operations. */
  public static let runningCompactions = DBProperty(rawValue: "rocksdb.num-running-compactions")

  /** The number of accumulated background errors that have occurred. */
  public static let backgroundErrors = DBProperty(rawValue: "rocksdb.background-errors")

  /** The approximate size of the active memtable, in bytes. */
  public static let activeMemtableSize = DBProperty(rawValue: "rocksdb.cur-size-active-mem-table")

  /** The approximate size of all unflushed memtables, in bytes. */
  public static let allMemtablesSize = DBProperty(rawValue: "rocksdb.cur-size-all-mem-tables")

  /** The total number of entries in the active memtable. */
  public static let activeMemtableEntries = DBProperty(rawValue: "rocksdb.num-entries-active-mem-table")

  /** The total number of entries in all unflushed memtables. */
  public static let unflushedMemtableEntries = DBProperty(rawValue: "rocksdb.num-entries-imm-mem-tables")

  /** The total number of delete entries in the active memtable. */
  public static let activeMemtableDeletes = DBProperty(rawValue: "rocksdb.num-deletes-active-mem-table")

  /** The total number of delete entries in all unflushed memtables. */
  public static let unflushedMemtableDeletes = DBProperty(rawValue: "rocksdb.num-deletes-imm-mem-tables")

  /** Returns the estimated number of total keys in the database. */
  public static let estimatedKeys = DBProperty(rawValue: "rocksdb.estimate-num-keys")

  /** The estimated amount of memory used for reading SST tables, excluding memory in block cache. */
  public static let tableReadersMemory = DBProperty(rawValue: "rocksdb.estimate-table-readers-mem")

  /** Returns 1 if file deletions are enabled, 0 otherwise. */
  public static let fileDeletionsEnabled = DBProperty(rawValue: "rocksdb.is-file-deletions-enabled")

  /** The number of unreleased snapshots in the database. */
  public static let snapshots = DBProperty(rawValue: "rocksdb.num-snapshots")

  /** When the oldest snapshot was created, as a UNIX timestamp. */
  public static let oldestSnapshot = DBProperty(rawValue: "rocksdb.oldest-snapshot-time")

  /** Returns the number of live versions in the database. See `version_set.h` for details. */
  public static let liveVersions = DBProperty(rawValue: "rocksdb.num-live-versions")

  /** The current LSM version, incremented after any change to the LSM tree. Not preserved across database restarts. */
  public static let superVersionNumber = DBProperty(rawValue: "rocksdb.current-super-version-number")

  /** The estimated amount of live data, in bytes. */
  public static let liveDataSize = DBProperty(rawValue: "rocksdb.estimate-live-data-size")

  /** The total size of all SST files, in bytes. */
  public static let sstFilesSize = DBProperty(rawValue: "rocksdb.total-sst-files-size")

  /** The level number that L0 data will be compacted to. */
  public static let baseLevel = DBProperty(rawValue: "rocksdb.base-level")

  /**
   * The estimated bytes that must be rewritten in order to reduce all levels to the target size.
   * Only valid for level-base compaction.
   */
  public static let pendingCompactionSize = DBProperty(rawValue: "rocksdb.estimate-pending-compaction-bytes")

  /** Creates a custom property descriptor. */
  public init(rawValue: RawValue) {
    self.rawValue = rawValue
  }

  /**
   * Creates a property that returns the number of files at the given level.
   *
   * - parameter atLevel: The level to identify.
   * - returns: A property descriptor.
   */
  public func files(atLevel level: Int) -> DBProperty {
    return DBProperty.init(rawValue: "rocksdb.num-files-at-level\(level)")
  }

  /**
   * Creates a property that returns the compression ratio of data at the given level.
   *
   * - parameter atLevel: The level to identify.
   * - returns: A property descriptor.
   */
  public func compressionRatio(atLevel level: Int) -> DBProperty {
    return DBProperty.init(rawValue: "rocksdb.compression-ratio-at-level\(level)")
  }
}

extension Database {

  /**
   * Returns a property describing some part of the database.
   * - seealso: DBProperty for a description of the available properties.
   *
   * - parameter property: The database property to retrieve.
   * - returns: The requested property, or nil if it couldn't be loaded.
   */
  public func getProperty(_ property: DBProperty) -> String? {
    let p = rocksdb_property_value(db, property.rawValue)
    guard let propVal = p else { return nil }
    defer { rocksdb_free(propVal) }
    return String(cString: propVal)
  }
}
