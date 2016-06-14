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
      /* RocksDBTests */
      ("testOpenFail", testOpenFail),
      ("testOpenForWriteFail", testOpenForWriteFail),
      ("testOpenForReadOnly", testOpenForReadOnly),
      ("testWriteFail", testWriteFail),
      ("testGetProperty", testGetProperty),
      ("testGetAndPut", testGetAndPut),
      ("testNilGet", testNilGet),
      ("testPutOverwrite", testPutOverwrite),
      ("testDelete", testDelete),

      /* DBBatchTests */
      ("testBatchWrite", testBatchWrite),
      ("testBatchMultiWrite", testBatchWrite),

      /* DBIteratorTests */
      ("testIterate", testIterate),
      ("testIteratePrefix", testIteratePrefix),

      /* DBOptionsTests */
      ("testStatistics", testStatistics),

      /* DBReadSnapshotTests */
      ("testReadSnapshot", testReadSnapshot),
    ]
  }
}
