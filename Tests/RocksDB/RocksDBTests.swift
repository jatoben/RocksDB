import XCTest
@testable import RocksDB

class RocksDBTests: XCTestCase {
  func testGetAndPut() {
    do {
      let db = try Database(path: "/tmp/test")
      try db.put("foo", value: "bar")
      let val = try db.get("foo")
      XCTAssertEqual(val!, "bar")
    } catch {
      XCTFail("\(error)")
    }
  }

  func testNilGet() {
    do {
      let db = try Database(path: "/tmp/test")
      let val = try db.get("baz")
      XCTAssertEqual(val, nil)
    } catch {
      XCTFail("\(error)")
    }
  }

  func testPutOverwrite() {
    do {
      let db = try Database(path: "/tmp/test")
      try db.put("foo", value: "bar")
      try db.put("foo", value: "baz")
      let val = try db.get("foo")
      XCTAssertEqual(val!, "baz")
    } catch {
      XCTFail("\(error)")
    }
  }

  static var allTests : [(String, (RocksDBTests) -> () throws -> Void)] {
    return [
      ("testGetAndPut", testGetAndPut),
      ("testNilGet", testNilGet),
      ("testPutOverwrite", testPutOverwrite)
    ]
  }
}
