import XCTest
@testable import RocksDB

class RocksDBTests: XCTestCase {
  func testExample() {
    do {
      let db = try Database(path: "/tmp/test")
      try db.put("foo", value: "bar")
      let val = try db.get("foo")
      XCTAssertEqual(val, "bar")
    } catch {
      XCTFail("\(error)")
    }
  }

  static var allTests : [(String, (RocksDBTests) -> () throws -> Void)] {
    return [
      ("testExample", testExample)
    ]
  }
}
