/*
 * RocksDBTests.swift
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
  func testOpenFail() {
    do {
      _ = try Database(path: "/foo/bar")
      XCTFail("Opening database at non-existent path should throw")
    } catch DBError.openFailed {
      /* success */
    } catch {
      XCTFail("Unexpected error type thrown: \(error)")
    }
  }

  func testOpenForWriteFail() {
    do {
      _ = try Database(path: dbPath)
      XCTFail("Opening database read-write a second time should throw")
    } catch DBError.openFailed {
      /* success */
    } catch {
      XCTFail("Unexpected error type thrown: \(error)")
    }
  }

  func testOpenForReadOnly() {
    do {
      let dbro = try Database(path: dbPath, readOnly: true)
      _ = try dbro.get("foo") as String?
      /* success */
    } catch {
      XCTFail("Should be able to open database read-only")
    }
  }

  func testWriteFail() {
    do {
      let dbro = try Database(path: dbPath, readOnly: true)
      try dbro.put("foo", value: "bar")
      XCTFail("Writing to a read-only database should throw")
    } catch DBError.writeFailed {
      /* success */
    } catch {
      XCTFail("Unexpected error type thrown: \(error)")
    }
  }

  func testGetProperty() {
    let prop = db.getProperty("rocksdb.estimate-num-keys")
    XCTAssertNotNil(prop, "`rocksdb.estimate-num-keys` property should not be nil")
  }

  func testGetAndPut() {
    do {
      try db.put("foo", value: "bar")
      let val = try db.get("foo") as String?
      XCTAssertEqual(val!, "bar")
    } catch {
      XCTFail("\(error)")
    }
  }

  func testNilGet() {
    do {
      let val = try db.get("baz") as String?
      XCTAssertEqual(val, nil)
    } catch {
      XCTFail("\(error)")
    }
  }

  func testPutOverwrite() {
    do {
      try db.put("foo", value: "bar")
      try db.put("foo", value: "baz")
      let val = try db.get("foo") as String?
      XCTAssertEqual(val!, "baz")
    } catch {
      XCTFail("\(error)")
    }
  }

  func testDelete() {
    do {
      try db.put("foo", value: "bar")
      let val = try db.get("foo") as String?
      XCTAssertEqual(val!, "bar")
      try db.delete("foo")
      let val2 = try db.get("foo") as String?
      XCTAssertNil(val2)
    } catch {
      XCTFail("\(error)")
    }
  }
}
