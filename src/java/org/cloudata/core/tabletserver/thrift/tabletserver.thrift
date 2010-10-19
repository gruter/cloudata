
service TabletServerService {
  void assignTablet(1:TabletInfo tabletInfo) throws CException;
  TabletInfo[] reportTablets();
  void printTabletInfo(1:string tabletName) throws CException;
  boolean checkServer();
  void splitForTest(1:TabletInfo tabletInfo) throws CException;
  void stopAllTablets() throws CException;
  void truncateColumn(1:string tabletName, 2:string columnName) throws CException;
  void addColumn(1:string tableName, 2:string tabletName, 3:string addedColumnName) throws CException;
  void doActionForTest(1:string tabletName, 2:string action) throws CException;
  boolean dropTable(1:string taskId, 2:string tableName) throws CException;
  void setProperty(1:string key, 2:string value) throws CException;
  string getProperty(1:string key, 2:string defaultValue) throws CException;
  void shutdown() throws CException;
  TabletInfo getTabletInfo(1:string tabletName) throws CException;
  TabletServerStatus getServerStatus() throws CException;
  TabletReport getTabletDetailInfo(1:string tabletName) throws CException;
  void doAllTabletMinorCompaction() throws CException;
  void electRebalacingTablets(1:int targetTabletNumPerTabletServer) throws CException;
  TabletInfo getTabletInfo(1:string tabletName);
  boolean isServicedTablet(1:string tabletName);
  long apply(1:string tabletName, 2:Row.Key rowKey, 3:TxId txId, 4:CommitLog[] commitLogList, 5:boolean saveLog) throws CException;
  boolean startFlush(1:string tabletName, 2:TxId txId) throws CException;
  void endFlush(1:string tabletName, 2:TxId txId) throws CException;
  ColumnValue get(1:string tabletName, 2:Row.Key rowKey, 
                                   3:string columnName, 4:Cell.Key columnKey) throws CException;
  ColumnValue[][] get(1:string tabletName, 2:Row.Key rowKey) throws CException;
  ColumnValue[][] get(1:string tabletName, 2:Row.Key rowKey, string[] columnNames) throws CException;
  ColumnValue getCloestMetaData(1:string tabletName, 2:Row.Key rowKey) throws CException;
  string[] getAllActions(1:string tabletName) throws CException;
  string getTabletServerConf(1:string key) throws CException;
  string test(1:long sleepTime, 2:string echo) throws CException;
  void saveTabletSplitedInfo(1:string tabletName, 2:TabletInfo targetTablet, 3:TabletInfo[] splitedTablets) throws CException;
  void printMeta(1:string tabletName) throws CException;
  void stopAction(1:string tableName, 2:string tabletActionClassName) throws CException;
  TabletReport getTabletDetailInfo(1:string tabletName) throws CException;
  boolean canUpload() throws CException;
  RowColumnValues[] get(1:string tabletName, 2:RowFilter scanFilter) throws CException;
  string startBatchUploader(1:string tabletName, 2:Row.Key rowKey, 3:boolean touch) throws CException;
  AsyncTaskStatus getAsyncTaskStatus(1:string taskId) throws CException;
  string endBatchUploader(1:string actionId, 2:string tabletName, 3:string[] columnNames, 4:string[] mapFileIds, 
      5:string[] mapFilePaths) throws CException;
  void cancelBatchUploader(1:string actionId, 2:string tabletName) throws CException;
  void touch(1:string leaseId) throws CException;
  ColumnValue[][] testRPC();
  ColumnValue[] getAllMemoryValues(1:string tabletName, 1:string columnName) throws CException;
  boolean hasValue(string 1:tabletName, string 2:columnName, Row.Key 3:rowKey, Cell.Key 4:cellKey) throws CException;
  Row.Key[] getSplitedRowKeyRanges(1:string tabletName, 2:int splitPerTablet) throws CException;
  RowColumnValues[][] gets(1:string tabletName, 2:Key startRowKey,
      3:RowFilter rowFilter) throws CException;
  ColumnValue[] getColumnMemoryCacheDatas(1:string tabletName, 2:string columnName) throws CException;
}
