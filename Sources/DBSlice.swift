/*
 * DBSlice.swift
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
  public var dbValue: [Int8] { return utf8CString.map { Int8($0) }}

  public init(dbValue: [Int8]) {
    var val = dbValue
    if val.last != 0 { val.append(0) }
    self.init(cString: val)
  }

  public init(_ dbEntry: DBEntry) {
    self.init(dbValue: dbEntry.dbValue)
  }
}
