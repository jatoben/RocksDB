/*
 * DBIteratorTests.swift
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
  func testIterate() {
    do {
      try db.put("foo", value: "bar")
      try db.put("baz", value: "quux")
      let kvs = db.map { ($0, $1) }
      XCTAssertTrue(kvs.count > 1, "Iterator didn't return enough keys")
      XCTAssertTrue(kvs.contains { (k, v) in String(k) == "foo" && String(v) == "bar" },
        "Iterator didn't contain expected key/value")
    } catch {
      XCTFail("\(error)")
    }
  }

  func testIteratePrefix() {
    do {
      try db.put("iterate:one", value: "bar")
      try db.put("iterate:two", value: "baz")
      try db.put("iterate:three", value: "quux")

      let iterator = db.makeIterator(keyPrefix: "iterate:")
      let kvs = IteratorSequence(iterator).map { ($0, $1) }
      XCTAssertEqual(kvs.count, 3, "Iterator returned wrong number of keys")
      XCTAssertTrue(kvs.contains { (k, _) in String(k).hasPrefix("iterate:") },
        "Iterator didn't contain expected keys")
    } catch {
      XCTFail("\(error)")
    }
  }
}
