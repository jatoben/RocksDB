/*
 * RocksDB.swift
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

public enum DBError: ErrorProtocol, CustomStringConvertible {
  case backupFailed(String)
  case openFailed(String)
  case readFailed(String)
  case restoreFailed(String)
  case writeFailed(String)

  public var description: String {
    switch self {
    case let backupFailed(s):
      return "Backup failed: \(s)"
    case let openFailed(s):
      return "Open failed: \(s)"
    case let readFailed(s):
      return "Read failed: \(s)"
    case let restoreFailed(s):
      return "Restore failed: \(s)"
    case let writeFailed(s):
      return "Write failed: \(s)"
    }
  }
}

public class Database {
  internal var db: OpaquePointer
  public let options: DBOptions
  public let path: String
  public let readOnly: Bool
  public lazy var readOptions = DBReadOptions()
  public lazy var writeOptions = DBWriteOptions()

  init(path: String, readOnly: Bool = false, options: DBOptions? = nil) throws {
    self.options = options ?? DBOptions()
    self.path = path
    self.readOnly = readOnly

    var err: UnsafeMutablePointer<Int8>? = nil
    var dbx = readOnly ?
      rocksdb_open_for_read_only(self.options.opts, path, 0, &err) :
      rocksdb_open(self.options.opts, path, &err)

    guard err == nil else {
      defer { rocksdb_free(err) }
      throw DBError.openFailed(String(cString: err!))
    }

    /* Need this rigamarole to avoid an IUO error because rocksdb_open is
     * typed to return an OpaquePointer!
     */
    guard dbx != nil else {
      throw DBError.openFailed("Unknown error")
    }

    db = dbx!
  }

  deinit {
    rocksdb_close(db)
  }

  public func getProperty(_ property: String) -> String? {
    let p = rocksdb_property_value(db, property)
    guard let propVal = p else { return nil }
    defer { rocksdb_free(propVal) }
    return String(cString: propVal)
  }

  public func put<K: DBSlice, V: DBSlice>(_ key: K, value: V, options: DBWriteOptions? = nil) throws {
    let opts = options ?? writeOptions

    let k = key.dbValue
    let v = value.dbValue
    var err: UnsafeMutablePointer<Int8>? = nil
    rocksdb_put(db, opts.opts, k, k.count, v, v.count, &err)

    guard err == nil else {
      defer { rocksdb_free(err) }
      throw DBError.writeFailed(String(cString: err!))
    }
  }

  public func write(_ batch: DBBatch, options: DBWriteOptions? = nil) throws {
    let opts = options ?? writeOptions
    var err: UnsafeMutablePointer<Int8>? = nil
    rocksdb_write(db, opts.opts, batch.batch, &err)

    guard err == nil else {
      defer { rocksdb_free(err) }
      throw DBError.writeFailed(String(cString: err!))
    }
  }

  public func delete<K: DBSlice>(_ key: K, options: DBWriteOptions? = nil) throws {
    let opts = options ?? writeOptions

    let k = key.dbValue
    var err: UnsafeMutablePointer<Int8>? = nil
    rocksdb_delete(db, opts.opts, k, k.count, &err)

    guard err == nil else {
      defer { rocksdb_free(err) }
      throw DBError.writeFailed(String(cString: err!))
    }
  }

  public func get<K: DBSlice, V: DBSlice>(_ key: K, options: DBReadOptions? = nil) throws -> V? {
    let opts = options ?? readOptions

    let k = key.dbValue
    var valLength: Int = 0
    var err: UnsafeMutablePointer<Int8>? = nil
    let value = rocksdb_get(db, opts.opts, k, k.count, &valLength, &err)

    guard err == nil else {
      defer { rocksdb_free(err) }
      throw DBError.readFailed(String(cString: err!))
    }

    guard let val = value else { return nil }
    defer { rocksdb_free(val) }
    let valPointer = UnsafeBufferPointer(start: val, count: valLength)
    return V(dbValue: [Int8](valPointer))
  }
}
