import XCTest
@testable import RocksDB

extension RocksDBTests {
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
}
