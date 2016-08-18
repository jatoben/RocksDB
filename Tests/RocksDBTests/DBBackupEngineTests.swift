/*
 * DBBackupEngineTests.swift
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
import XCTest
@testable import RocksDB

extension RocksDBTests {
  func testCreateBackup() {
    do {
      let be = try db.createBackupEngine(path: backupPath)
      XCTAssertEqual(be.listBackups().count, 0, "Should have 0 backups")

      let before = Int64(time(nil))
      try be.createBackup()
      let backups = be.listBackups()
      XCTAssertEqual(backups.count, 1, "Should have 1 backup")

      let backup = backups.first!
      XCTAssertGreaterThan(backup.backupId, 0, "Invalid backup ID")
      XCTAssertGreaterThan(backup.numberOfFiles, 0, "Backup should have at least 1 file")
      XCTAssertGreaterThan(backup.size, 0, "Backup should have a non-zero size")
      XCTAssertGreaterThanOrEqual(backup.timestamp, before, "Bad backup timestamp")

      try be.createBackup()
      XCTAssertEqual(be.listBackups().count, 2, "Should have 2 backups")
    } catch {
      XCTFail("\(error)")
    }
  }

  func testPurgeOldBackups() {
    do {
      let be = try db.createBackupEngine(path: backupPath)
      for _ in 0..<5 { try be.createBackup() }
      XCTAssertEqual(be.listBackups().count, 5, "Should have 5 backups before purge")

      try be.purgeBackups(keepMostRecent: 2)
      XCTAssertEqual(be.listBackups().count, 2, "Should have 2 backups after purge")
    } catch {
      XCTFail("\(error)")
    }
  }

  func testRestoreBackup() {
    do {
      let be = try db.createBackupEngine(path: backupPath)

      try db.put("foo", value: "bar")
      try be.createBackup()
      try db.put("baz", value: "quux")

      XCTAssertEqual(try (db.get("foo") as String?)!, "bar", "Failed to read key before backup")
      XCTAssertEqual(try (db.get("baz") as String?)!, "quux", "Failed to read key before backup")

      try be.restoreLatestBackup()
      XCTAssertEqual(try (db.get("foo") as String?)!, "bar", "Failed to read key after restore")
      XCTAssertNil(try db.get("bar") as String?, "Read invalid key after restore")
    } catch {
      XCTFail("\(error)")
    }
  }

  func testRestoreNonExistentBackup() {
    do {
      let be = try db.createBackupEngine(path: backupPath)
      XCTAssertEqual(be.listBackups().count, 0, "Should have 0 backups")
      try be.restoreLatestBackup()
    } catch DBError.backupFailed {
      /* success */
    } catch {
      XCTFail("\(error)")
    }
  }
}
