import XCTest
import CRocksDB
@testable import RocksDB

class FailureTests: XCTestCase {
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

  func testOpenForReadOnly() {
    do {
      _ = try Database(path: dbPath, readOnly: true)
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

  static var allTests : [(String, (FailureTests) -> () throws -> Void)] {
    return [
      ("testOpenFail", testOpenFail),
      ("testOpenForWriteFail", testOpenForWriteFail),
      ("testOpenForReadOnly", testOpenForReadOnly),
      ("testWriteFail", testWriteFail),
    ]
  }
}
