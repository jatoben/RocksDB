public protocol DBSlice {
  var dbValue: [Int8] { get }

  init(dbValue: [Int8])
}

public struct DBEntry {
  public private(set) var dbValue: [Int8]

  init(dbValue: [Int8]) {
    self.dbValue = dbValue
  }
}

extension String: DBSlice {
  public var dbValue: [Int8] { return nulTerminatedUTF8.map { Int8($0) }}

  public init(dbValue: [Int8]) {
    var val = dbValue
    if val.last != 0 { val.append(0) }
    self.init(cString: val)
  }

  public init(_ dbEntry: DBEntry) {
    self.init(dbValue: dbEntry.dbValue)
  }
}
