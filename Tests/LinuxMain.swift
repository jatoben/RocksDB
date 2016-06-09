import XCTest
@testable import RocksDBTestSuite

XCTMain([
  testCase(RocksDBTests.allTests),
])
