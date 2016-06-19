/*
 * DBBatch.swift
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

public class DBBatch {
  internal var batch: OpaquePointer

  public var count: Int {
    return Int(rocksdb_writebatch_count(batch))
  }

  public init() {
    batch = rocksdb_writebatch_create()
  }

  deinit {
    rocksdb_writebatch_destroy(batch)
  }

  public func put(_ key: DBSlice, value: DBSlice) {
    let k = key.dbValue
    let v = value.dbValue
    rocksdb_writebatch_put(batch, k, k.count, v, v.count)
  }

  public func put<K: DBSlice, V: DBSlice>(_ entries: [K: V]) {
    entries.forEach { put($0, value: $1) }
  }

  public func delete(_ key: DBSlice) {
    let k = key.dbValue
    rocksdb_writebatch_delete(batch, k, k.count)
  }

  public func delete<S: Sequence where S.Iterator.Element == DBSlice>(_ keys: S, options: DBWriteOptions? = nil) {
    keys.forEach { delete($0) }
  }
}
