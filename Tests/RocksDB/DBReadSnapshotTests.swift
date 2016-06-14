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
      readOpts.setReadSnapshot(snap)
      XCTAssertEqual(try db.get("foo-snap", options: readOpts) as String?, "bar", "Didn't read expected value with read snapshot")
      XCTAssertNil(try db.get("baz-snap", options: readOpts) as String?, "Read unexpected value from read snapshot")
      XCTAssertEqual(try db.get("baz-snap") as String?, "quux", "Didn't read expected value without read snapshot")
    } catch {
      XCTFail("\(error)")
    }
  }
}
