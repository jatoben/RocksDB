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

  static var allTests : [(String, (RocksDBTests) -> () throws -> Void)] {
    return [
      /* BasicTests */
      ("testGetAndPut", testGetAndPut),
      ("testNilGet", testNilGet),
      ("testPutOverwrite", testPutOverwrite),
      ("testDelete", testDelete),
      ("testBatchWrite", testBatchWrite),
      ("testBatchMultiWrite", testBatchWrite),
      ("testIterate", testIterate),
      ("testIteratePrefix", testIteratePrefix),
      ("testReadSnapshot", testReadSnapshot),

      /* FailureTests */
      ("testOpenFail", testOpenFail),
      ("testOpenForWriteFail", testOpenForWriteFail),
      ("testOpenForReadOnly", testOpenForReadOnly),
      ("testWriteFail", testWriteFail),
    ]
  }
}
