import XCTest
@testable import RocksDB

class RocksDBTests: XCTestCase {
  var dbPath: String = "/tmp/rocksdb-test"

  func testGetAndPut() {
    do {
      let db = try Database(path: dbPath)
      try db.put("foo", value: "bar")
      let val = try db.get("foo") as String?
      XCTAssertEqual(val!, "bar")
    } catch {
      XCTFail("\(error)")
    }
  }

  func testNilGet() {
    do {
      let db = try Database(path: dbPath)
      let val = try db.get("baz") as String?
      XCTAssertEqual(val, nil)
    } catch {
      XCTFail("\(error)")
    }
  }

  func testPutOverwrite() {
    do {
      let db = try Database(path: dbPath)
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
      let db = try Database(path: dbPath)
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

      let db = try Database(path: dbPath)
      let batch = DBBatch()
      for i in 0..<keys.count {
        batch.put(keys[i], value: vals[i])
      }
      XCTAssertEqual(batch.count, keys.count)
      try db.write(batch)

      for i in 0..<keys.count {
        let val = try db.get(keys[i]) as String?
        XCTAssertEqual(val!, vals[i])
      }
    } catch {
      XCTFail("\(error)")
    }
  }

  func testIterate() {
    do {
      let db = try Database(path: dbPath)
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
      let db = try Database(path: dbPath)
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
