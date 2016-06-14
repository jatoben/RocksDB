import Foundation
import XCTest
@testable import RocksDB

let DBPath = "/tmp/rocksdb-test"
let PI = ProcessInfo()
let FM = FileManager.default()

class RocksDBTests: XCTestCase {
  var dbPath = DBPath + "/db"
  var db: Database!

  /* When running tests from Xcode, the working directory is changed and the SRCROOT env var
   * isn't available by default, but we need it to find the ramdisk scripts. To fix that, edit
   * the test scheme and in the Arguments pane, set an env var of SRCROOT to the value $SRCROOT,
   * and choose to expand variables based on the RocksDB product.
   *
   * Swift Package Manager doesn't change the working dir, so we can just use the
   * currentDirectoryPath when testing in that environment.
   */
  class func sourcePath() -> String {
    return PI.environment["SRCROOT"] ?? FM.currentDirectoryPath
  }

  /* Mount a RAM disk to store database files during a test run */
  override class func setUp() {
    #if os(OSX)
      let task = Task.launchedTask(withLaunchPath: sourcePath() + "/create-ramdisk-macos.sh", arguments: [DBPath])
      task.waitUntilExit()
      assert(task.terminationStatus == 0)
    #endif
  }

  /* Nuke the RAM disk */
  override class func tearDown() {
    #if os(OSX)
      let task = Task.launchedTask(withLaunchPath: sourcePath() + "/remove-ramdisk-macos.sh", arguments: [DBPath])
      task.waitUntilExit()
      assert(task.terminationStatus == 0)
    #endif
  }

  override func setUp() {
    try! FM.createDirectory(atPath: dbPath, withIntermediateDirectories: true, attributes: nil)
    db = try! Database(path: dbPath)
  }

  override func tearDown() {
    db = nil
    try! FM.removeItem(atPath: dbPath)
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
