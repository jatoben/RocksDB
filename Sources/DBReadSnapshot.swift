import CRocksDB

public class DBReadSnapshot {
  private var db: Database
  internal var snapshot: OpaquePointer

  internal init(_ db: Database) {
    self.db = db
    snapshot = rocksdb_create_snapshot(db.db)
  }

  deinit {
    rocksdb_release_snapshot(db.db, snapshot)
  }
}

extension Database {
  public func createReadSnapshot() -> DBReadSnapshot {
    return DBReadSnapshot(self)
  }
}
