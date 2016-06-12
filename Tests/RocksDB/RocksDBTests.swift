import XCTest
@testable import RocksDB

class RocksDBTests: XCTestCase {
  var dbPath: String = "/tmp/rocksdb-test"
  var db: Database!

  override func setUp() {
    db = try! Database(path: dbPath)
  }

  override func tearDown() {
    db = nil
  }

  func testOpenFail() {
    do {
      _ = try Database(path: "/foo/bar")
      XCTFail("Opening database at non-existent path should throw")
    } catch DBError.OpenFailed {
      /* success */
    } catch {
      XCTFail("Unexpected error type thrown: \(error)")
    }
  }

  func testOpenForWriteFail() {
    do {
      _ = try Database(path: dbPath)
      XCTFail("Opening database read-write a second time should throw")
    } catch DBError.OpenFailed {
      /* success */
    } catch {
      XCTFail("Unexpected error type thrown: \(error)")
    }
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

  func testIterate() {
    do {
      try db.put("foo", value: "bar")
      let kvs = db.map { ($0, $1) }
      XCTAssertTrue(kvs.count > 1, "Iterator didn't return enough keys")
      XCTAssertTrue(kvs.contains { (k, v) in String(k) == "foo" && String(v) == "bar" }, "Iterator didn't contain expected key/value")
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
      XCTAssertTrue(kvs.contains { (k, _) in String(k).hasPrefix("iterate:") }, "Iterator didn't contain expected keys")
    } catch {
      XCTFail("\(error)")
    }
  }

  func testReadSnapshot() {
    do {
      try db.put("foo", value: "bar")
      let snap = db.createReadSnapshot()
      try db.put("baz", value: "quux")

      let readOpts = DBReadOptions()
      readOpts.setReadSnapshot(snap)
      XCTAssertEqual(try db.get("foo", options: readOpts) as String?, "bar", "Didn't read expected value with read snapshot")
      XCTAssertNil(try db.get("baz", options: readOpts) as String?, "Read unexpected value from read snapshot")
      XCTAssertEqual(try db.get("baz") as String?, "quux", "Didn't read expected value without read snapshot")
    } catch {
      XCTFail("\(error)")
    }
  }

  static var allTests : [(String, (RocksDBTests) -> () throws -> Void)] {
    return [
      ("testGetAndPut", testGetAndPut),
      ("testNilGet", testNilGet),
      ("testPutOverwrite", testPutOverwrite),
      ("testDelete", testDelete),
      ("testBatchWrite", testBatchWrite),
      ("testIterate", testIterate),
      ("testIteratePrefix", testIteratePrefix)
    ]
  }
}
