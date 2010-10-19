exception CException {
}

struct ColumnInfoThrift {
}

struct DiskInfoThrift {
}

struct TableSchemaThrift {
  1: string tableName;
  2: string description;
  3: list<ColumnInfoThrift> columns;
  4: i32 numOfVersion;
  5: string owner;
  6: map<string, string> permissions;
}

struct TabletInfoThrift {
  1: string tableName;
  2: string tabletName;
  3: string startRowKey;
  4: string endRowKey;
  5: string assignedHostName;
}

struct TabletServerInfoThrift {
  1: string hostName;
  2: i64 lastHeartbeatTime;
  3: i32 numOfTablets;
}

struct ServerMonitorInfoThrift {
  1: DiskInfoThrift diskInfo;
  2: i64 logDirUsed;
  3: string logPath;
  4: i64 freeMemory;
  5: i64 totalMemory;
  6: i64 maxMemory;
  7: i64 serverStartTime;
  8: list<string> logFiles;
  9: i64 lastHeartbeatTime;
}