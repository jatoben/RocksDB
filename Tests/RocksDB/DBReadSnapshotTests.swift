/*
 * DBReadSnapshotTests.swift
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
  func testReadSnapshot() {
    do {
      try db.put("foo-snap", value: "bar")
      try db.delete("baz-snap")
      let snap = db.createReadSnapshot()
      try db.put("baz-snap", value: "quux")

      let readOpts = DBReadOptions()
      readOpts.readSnapshot = snap
      XCTAssertEqual(try db.get("foo-snap", options: readOpts) as String?, "bar", "Didn't read expected value with read snapshot")
      XCTAssertNil(try db.get("baz-snap", options: readOpts) as String?, "Read unexpected value from read snapshot")
      XCTAssertEqual(try db.get("baz-snap") as String?, "quux", "Didn't read expected value without read snapshot")
    } catch {
      XCTFail("\(error)")
    }
  }
}
