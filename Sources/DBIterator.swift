import CRocksDB

public class DBIterator: IteratorProtocol {
  private var iter: OpaquePointer

  internal init(_ iter: OpaquePointer, _ keyPrefix: String? = nil) {
    self.iter = iter

    if let prefix = keyPrefix {
      rocksdb_iter_seek(iter, prefix, prefix.utf8.count)
    } else {
      rocksdb_iter_seek_to_first(self.iter)
    }
  }

  deinit {
    rocksdb_iter_destroy(iter)
  }

  public func next() -> (DBEntry, DBEntry)? {
    var keyLength: Int = 0
    var valLength: Int = 0

    guard rocksdb_iter_valid(iter) != 0 else { return nil }

    let k = rocksdb_iter_key(iter, &keyLength)
    let v = rocksdb_iter_value(iter, &valLength)
    guard let key = k, let val = v else { return nil }

    defer { rocksdb_iter_next(iter) }
    let keyPointer = UnsafeBufferPointer(start: key, count: keyLength)
    let valPointer = UnsafeBufferPointer(start: val, count: valLength)
    return (DBEntry(dbValue: [Int8](keyPointer)), DBEntry(dbValue: [Int8](valPointer)))
  }
}

extension Database: Sequence {
  public func makeIterator(_ opts: DBReadOptions, keyPrefix prefix: String? = nil) -> DBIterator {
    let i = rocksdb_create_iterator(db, opts.opts)
    guard let iter = i else { preconditionFailure("Could not create database iterator") }
    return DBIterator(iter, prefix)
  }

  public func makeIterator(keyPrefix prefix: String) -> DBIterator {
    return makeIterator(defaultReadOptions, keyPrefix: prefix)
  }

  public func makeIterator() -> DBIterator {
    return makeIterator(defaultReadOptions)
  }
}
