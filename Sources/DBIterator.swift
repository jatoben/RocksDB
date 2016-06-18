/*
 * DBIterator.swift
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

public class DBIterator: IteratorProtocol {
  private var iter: OpaquePointer

  private init(_ iter: OpaquePointer, _ keyPrefix: String? = nil) {
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

  public func next() -> (DBEntry, DBEntry)? {
    guard rocksdb_iter_valid(iter) != 0 else { return nil }

    var keyLength: Int = 0
    var valLength: Int = 0

    let k = rocksdb_iter_key(iter, &keyLength)
    let v = rocksdb_iter_value(iter, &valLength)
    guard let key = k, let val = v else { return nil }

    defer { rocksdb_iter_next(iter) }
    let keyPointer = UnsafeBufferPointer(start: key, count: keyLength)
    let valPointer = UnsafeBufferPointer(start: val, count: valLength)
    return (DBEntry(dbValue: [Int8](keyPointer)), DBEntry(dbValue: [Int8](valPointer)))
  }
}

extension Database: Sequence {
  public func makeIterator(_ opts: DBReadOptions, keyPrefix prefix: String? = nil) -> DBIterator {
    let i = rocksdb_create_iterator(db, opts.opts)
    guard let iter = i else { preconditionFailure("Could not create database iterator") }
    return DBIterator(iter, prefix)
  }

  public func makeIterator(keyPrefix prefix: String) -> DBIterator {
    return makeIterator(readOptions, keyPrefix: prefix)
  }

  public func makeIterator() -> DBIterator {
    return makeIterator(readOptions)
  }
}
