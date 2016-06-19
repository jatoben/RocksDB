/*
 * DBBackupEngine.swift
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

public struct BackupInfo {
  public let backupId: Int
  public let numberOfFiles: Int
  public let size: UInt64
  public let timestamp: Int64
}

public class DBRestoreOptions {
  internal var opts: OpaquePointer = rocksdb_restore_options_create()

  public var keepLogFiles: Bool = false {
    didSet {
      rocksdb_restore_options_set_keep_log_files(opts, keepLogFiles ? 1 : 0)
    }
  }

  deinit {
    rocksdb_restore_options_destroy(opts)
  }
}

public class DBBackupEngine {
  private var db: Database
  private var engine: OpaquePointer
  public let path: String

  internal init(database db: Database, path: String) throws {
    self.db = db
    self.path = path

    var err: UnsafeMutablePointer<Int8>? = nil
    var be = rocksdb_backup_engine_open(db.options.opts, path, &err)

    guard err == nil else {
      defer { rocksdb_free(err) }
      throw DBError.backupFailed(String(cString: err!))
    }

    guard be != nil else {
      throw DBError.backupFailed("Unknown error")
    }

    engine = be!
  }

  deinit {
    rocksdb_backup_engine_close(engine)
  }

  public func createBackup() throws {
    var err: UnsafeMutablePointer<Int8>? = nil
    rocksdb_backup_engine_create_new_backup(engine, db.db, &err)

    guard err == nil else {
      defer { rocksdb_free(err) }
      throw DBError.backupFailed(String(cString: err!))
    }
  }

  public func listBackups() -> [BackupInfo] {
    let info = rocksdb_backup_engine_get_backup_info(engine)
    defer { rocksdb_backup_engine_info_destroy(info) }

    var backups = [BackupInfo]()
    let count = rocksdb_backup_engine_info_count(info)
    backups.reserveCapacity(Int(count))

    for i in 0..<count {
      let id = rocksdb_backup_engine_info_backup_id(info, i)
      let numFiles = rocksdb_backup_engine_info_number_files(info, i)
      let size = rocksdb_backup_engine_info_size(info, i)
      let timestamp = rocksdb_backup_engine_info_timestamp(info, i)

      backups.append(BackupInfo(backupId: Int(id),
        numberOfFiles: Int(numFiles),
        size: size,
        timestamp: timestamp))
    }

    return backups
  }

  public func purgeBackups(keepMostRecent keep: Int) throws {
    var err: UnsafeMutablePointer<Int8>? = nil
    rocksdb_backup_engine_purge_old_backups(engine, UInt32(keep), &err)

    guard err == nil else {
      defer { rocksdb_free(err) }
      throw DBError.backupFailed(String(cString: err!))
    }
  }

  public func restoreLatestBackup(options: DBRestoreOptions? = nil) throws {
    let opts = options ?? DBRestoreOptions()
    var err: UnsafeMutablePointer<Int8>? = nil
    rocksdb_backup_engine_restore_db_from_latest_backup(engine,
      db.path,
      db.path,
      opts.opts,
      &err)

    guard err == nil else {
      defer { rocksdb_free(err) }
      throw DBError.backupFailed(String(cString: err!))
    }
  }
}

extension Database {
  public func createBackupEngine(path: String) throws -> DBBackupEngine {
    return try DBBackupEngine(database: self, path: path)
  }
}
