/*
 * DBReadSnapshot.swift
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

public class DBReadSnapshot {
  private var db: Database
  internal var snapshot: OpaquePointer

  internal init(_ db: Database) {
    self.db = db
    snapshot = rocksdb_create_snapshot(db.db)
  }

  deinit {
    rocksdb_release_snapshot(db.db, snapshot)
  }
}

extension Database {
  public func createReadSnapshot() -> DBReadSnapshot {
    return DBReadSnapshot(self)
  }
}
