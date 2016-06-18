/*
 * DBBatchTests.swift
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
  func testBatchWrite() {
    do {
      let keys = ["foo-batch-1", "foo-batch-2", "foo-batch-3"]
      let vals = ["bar", "baz", "quux"]

      try db.put("foo", value: "bar")
      let batch = DBBatch()
      for i in 0..<keys.count {
        batch.put(keys[i], value: vals[i])
      }
      batch.delete("foo")
      XCTAssertEqual(batch.count, keys.count + 1)
      try db.write(batch)

      for i in 0..<keys.count {
        let val = try db.get(keys[i]) as String?
        XCTAssertEqual(val!, vals[i])
      }

      XCTAssertNil(try db.get("foo") as String?, "Key not deleted in batch write")
    } catch {
      XCTFail("\(error)")
    }
  }

  func testBatchMultiWrite() {
    do {
      let entries = [
        "foo-batch-1": "bar",
        "foo-batch-2": "baz",
        "foo-batch-3": "quux"
      ]

      let batch = DBBatch()
      batch.put(entries)
      batch.delete(["foo-batch-2", "foo-batch-3"])
      try db.write(batch)

      XCTAssertEqual(try db.get("foo-batch-1") as String!, "bar", "Key shouldn't have been deleted in batch delete")
      XCTAssertNil(try db.get("foo-batch-2") as String?, "Key not deleted in batch delete")
      XCTAssertNil(try db.get("foo-batch-3") as String?, "Key not deleted in batch delete")
    } catch {
      XCTFail("\(error)")
    }
  }
}
