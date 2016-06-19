/*
 * AllTests.swift
 * Copyright (c) 2016 Ben Gollmer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import Foundation
import XCTest
@testable import RocksDB

let DBPath = "/tmp/rocksdb-test"
let PI = ProcessInfo()
let FM = FileManager.default()

class RocksDBTests: XCTestCase {
  var backupPath = DBPath + "/backups"
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
    try! FM.createDirectory(atPath: backupPath, withIntermediateDirectories: true, attributes: nil)
    try! FM.createDirectory(atPath: dbPath, withIntermediateDirectories: true, attributes: nil)
    db = try! Database(path: dbPath)
  }

  override func tearDown() {
    db = nil
    try! FM.removeItem(atPath: backupPath)
    try! FM.removeItem(atPath: dbPath)
  }

  static var allTests : [(String, (RocksDBTests) -> () throws -> Void)] {
    return [
      /* RocksDBTests */
      ("testOpenFail", testOpenFail),
      ("testOpenForWriteFail", testOpenForWriteFail),
      ("testOpenForReadOnly", testOpenForReadOnly),
      ("testWriteFail", testWriteFail),
      ("testGetAndPut", testGetAndPut),
      ("testNilGet", testNilGet),
      ("testPutOverwrite", testPutOverwrite),
      ("testDelete", testDelete),

      /* DBBackupEngineTests */
      ("testCreateBackup", testCreateBackup),
      ("testPurgeOldBackups", testPurgeOldBackups),
      ("testRestoreBackup", testRestoreBackup),
      ("testRestoreNonExistentBackup", testRestoreNonExistentBackup),

      /* DBBatchTests */
      ("testBatchWrite", testBatchWrite),
      ("testBatchMultiWrite", testBatchWrite),

      /* DBIteratorTests */
      ("testIterate", testIterate),
      ("testIteratePrefix", testIteratePrefix),

      /* DBPropertyTests */
      ("testGetProperty", testGetProperty),
      ("testGetCustomProperty", testGetCustomProperty),
      ("testGetInvalidProperty", testGetInvalidProperty),

      /* DBOptionsTests */
      ("testStatistics", testStatistics),

      /* DBReadSnapshotTests */
      ("testReadSnapshot", testReadSnapshot),
    ]
  }
}
