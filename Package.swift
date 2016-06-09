import PackageDescription

let package = Package(
  name: "RocksDB",
  dependencies: [
    .Package(url: "../CRocksDB", majorVersion: 1)
  ]
)
