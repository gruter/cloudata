#!/usr/local/bin/thrift -java -cpp
#
# Thrift Service exported by Cloudata
#

/**
 * The first thing to know about are types. The available types in Thrift are:
 *
 *  bool        Boolean, one byte
 *  byte        Signed byte
 *  i16         Signed 16-bit integer
 *  i32         Signed 32-bit integer
 *  i64         Signed 64-bit integer
 *  double      64-bit floating point value
 *  string      String
 *  binary      Blob (byte array)
 *  map<t1,t2>  Map from one type to another
 *  list<t1>    Ordered list of one type
 *  set<t1>     Set of unique elements of one type
 *
 * Did you also notice that Thrift supports C style comments?
 */

namespace cpp cloudata
namespace java org.cloudata.thrift.generated
namespace php cloudata
namespace perl cloudata

struct ThriftHandle {
  1: string id,
}

struct ThriftColumnInfo {
  1: string columnName,
  2: string numOfVersion,
  3: string columnType,
}

struct ThriftTableSchema {
  1: string tableName,
  2: string description,
  3: string numOfVersion,
  4: string owner,
  5: list<ThriftColumnInfo> columns,
}

struct ThriftCellValue {
  1: binary data,
  2: string timestamp,
  3: bool deleted,
}

struct ThriftCell {
  1: string cellKey,
  2: list<ThriftCellValue> values,
}

struct ThriftRow {
  1: string rowKey,
  2: map<string,list<ThriftCell>> cells,
}

exception ThriftIOException {
  1: string message
}

service ThriftCloudataService
{
  list<ThriftTableSchema> listTables() throws (1:ThriftIOException tioe),
  void put(1:string tableName, 2:ThriftRow row, 3:bool useSystemTimestamp) throws (1:ThriftIOException tioe),
  void removeRow(1:string tableName, 2:string rowKey) throws (1:ThriftIOException tioe),
  void removeRowWithTime(1:string tableName, 2:string rowKey, 3:string timestamp) throws (1:ThriftIOException tioe),
  void remove(1:string tableName, 2:string rowKey, 3:string columnName, 4:string cellKey) throws (1:ThriftIOException tioe),
  bool hasValue(1:string tableName, 2:string columnName, 3:string rowKey) throws (1:ThriftIOException tioe),
  binary getValue(1:string tableName, 2:string rowKey, 3:string columnName, 4:string cellKey) throws (1:ThriftIOException tioe),
  ThriftRow get(1:string tableName, 2:string rowKey) throws (1:ThriftIOException tioe),
  ThriftRow getColumnRow(1:string tableName, 2:string rowKey, 3:list<string> columnNames) throws (1:ThriftIOException tioe),
  void createTable(1:ThriftTableSchema tableSchema) throws (1:ThriftIOException tioe),
  bool existsTable(1:string tableName) throws (1:ThriftIOException tioe),
  void dropTable(1:string tableName) throws (1:ThriftIOException tioe),
  void truncateTable(1:string tableName, 2:bool clearPartitionInfo) throws (1:ThriftIOException tioe),
  void truncateColumn(1:string tableName, 2:string columnName) throws (1:ThriftIOException tioe),
  ThriftTableSchema descTable(1:string tableName) throws (1:ThriftIOException tioe),
  void addColumn(1:string tableName 2:string addedColumnName) throws (1:ThriftIOException tioe),
}