import XCTest
@testable import RocksDB

extension RocksDBTests {
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
    } catch DBError.WriteFailed {
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
}
