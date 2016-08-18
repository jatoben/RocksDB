/*
 * DBOptionsTests.swift
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
import XCTest
@testable import RocksDB

extension RocksDBTests {
  func testStatistics() {
    do {
      db = nil
      let opts = DBOptions()
      opts.enableStatistics()

      db = try Database(path: dbPath, options: opts)
      try db.put("foo", value: "bar")
      try db.put("stats", value: "true")

      let stats = opts.getStatistics()
      XCTAssertNotNil(stats, "Enabled stats should not be nil")
    } catch {
      XCTFail("\(error)")
    }
  }
}
