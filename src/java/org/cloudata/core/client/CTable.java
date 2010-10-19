/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.cloudata.core.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.client.blob.BlobColumnCell;
import org.cloudata.core.client.blob.BlobMetaCell;
import org.cloudata.core.client.blob.BlobMetaManager;
import org.cloudata.core.client.blob.NBlobInputStream;
import org.cloudata.core.client.blob.CBlobOutputStream;
import org.cloudata.core.client.scanner.ScannerFactory;
import org.cloudata.core.client.scanner.TableScanner;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.exception.GetFailException;
import org.cloudata.core.common.exception.PermissionException;
import org.cloudata.core.fs.CloudataFileSystem;
import org.cloudata.core.fs.GPath;
import org.cloudata.core.master.TableManagerProtocol;
import org.cloudata.core.tablet.ColumnValue;
import org.cloudata.core.tablet.RowColumnValues;
import org.cloudata.core.tablet.TableSchema;
import org.cloudata.core.tablet.TabletInfo;
import org.cloudata.core.tablet.TabletReport;
import org.cloudata.core.tablet.TableSchema.ColumnInfo;
import org.cloudata.core.tabletserver.CommitLog;
import org.cloudata.core.tabletserver.DataServiceProtocol;
import org.cloudata.core.tabletserver.Tablet;
import org.cloudata.core.tabletserver.TxId;


/**
 * CTable represents a table in Cloudata and each table contains data of applications 
 * with distributed and persistent manner. This class provides applications with convenient way 
 * to handle its data, allowing applications to get and put the data as if they are located 
 * in local.<p>
 * <b>CTable class is not thread safe, but it is permitted for each thread to own CTable instance
 * and concurrently operate with it.</b>    
 */

public class CTable implements Constants {
  static final int TX_MAX_RETRY = 2;

  private static final Log LOG = LogFactory.getLog(CTable.class.getName());
  
  private CloudataConf conf;

  private TableSchema tableSchema;
  protected TabletLocationCache locationCache;
  private TxOperation txOperation;
  private int txTimeout;
  private int pauseTime = 20;
  private String tableName;
  private Set<String> columnSet = new HashSet<String>();
  
  static String userId = System.getProperty("user.name");
  
  private boolean autoFlush = true;
  
  private int maxBufferSize;
  
  private int bufferSize;
  
  private int bufferWriteSize;
  
  private int maxResultRecord;
  
  private int minNumOfCharsForLikeOperation;
  /**
   * Returns CTable instance if exists, otherwise returns null.
   * @param conf Configuration variables of Cloudata
   * @param tableName table name
   * @return CTable instance
   * @throws IOException
   */
  public static CTable openTable(CloudataConf conf, String tableName) throws IOException {
    setUserId(conf);
    
    if(TabletLocationCache.getInstance(conf).getTableSchema(tableName) == null) {
    //if(!CTableManager.existsTable(conf, tableName)) {
      return null;
    } else {
      CTable ctable = new CTable(conf, tableName);
      return ctable;
    }
  }
  
  /**
   * Returns CTable instance if exists, otherwise returns null.
   * @param conf Configuration variables of Cloudata
   * @param tableSchema
   * @param create true, if you want to create a new table when it does not exist.
   * @return CTable instance
   * @throws IOException
   */
  public static CTable openTable(CloudataConf conf, TableSchema tableSchema, boolean create) throws IOException {
    setUserId(conf);
    //if(!CTableManager.existsTable(conf, tableSchema.getTableName())) {
    if(TabletLocationCache.getInstance(conf).getTableSchema(tableSchema.getTableName()) == null) {
      if(create) {
        CTable.createTable(conf, tableSchema);
      } else {
        throw new IOException("Table [" + tableSchema.getTableName() + "] not exists");
      }
    }
    
    CTable ctable = CTable.openTable(conf, tableSchema.getTableName());
    return ctable;
  }
  
  /**
   * Check if table exists.
   * @param conf Configuration variables of Cloudata
   * @param tableName table name
   * @return true if the table exists, otherwise false.
   * @throws IOException
   */
  public static boolean existsTable(CloudataConf conf, String tableName) throws IOException {
    setUserId(conf);
    return CTableManager.existsTable(conf, tableName);
  }
  
  /**
   * Returns schema information of all the tables in Cloudata
   * @param conf Configuration variables of Cloudata
   * @return TableInfo[] the list of all the table schema information
   * @throws IOException
   */
  public static TableSchema[] listTables(CloudataConf conf) throws IOException {
    return CTableManager.listTables(conf);
  }
  
  private static void setUserId(CloudataConf conf) {
    if("y".equals(conf.get("cloudata.jdbc"))) {
      return;
    } else {
      conf.setUserId(userId);
    }
  }
  
  private CTable(CloudataConf conf, String tableName) throws IOException {
    this.conf = conf;
    setUserId(this.conf); 
    
    this.tableName = tableName;
    this.txTimeout = conf.getInt("client.tx.timeout", 60) * 1000;
    this.maxBufferSize = conf.getInt("client.maxBufferSize", 24 * 1024 * 1024);  //24MB
    this.bufferSize = conf.getInt("client.bufferSize", 1024 * 1024);  //1MB
    this.locationCache = TabletLocationCache.getInstance(conf);
    this.txOperation = new TxOperation();
    this.tableSchema = locationCache.getTableSchema(tableName);
    if(tableSchema == null) {
      throw new IOException("Wrong table schema [" + tableName + "]. check schema info on zookeeper");
    }
    
    for(String column: tableSchema.getColumnsArray()) {
      columnSet.add(column);
    }
    
    this.maxResultRecord = conf.getInt("client.max.resultRecord", 5000);
    this.minNumOfCharsForLikeOperation = conf.getInt("client.min.numOfChars.like", 4);
  }

  private void checkColumn(String columnName) throws IOException {
    if(columnName == null) {
      throw new IOException("Column name is null");
    }
    if(!columnSet.contains(columnName)) {
      throw new IOException("Invalid columnName [" + columnName + "] in table [" + tableName + "]");
    }
  }

  private void checkColumn(String[] columnNames) throws IOException {
    if(columnNames == null) {
      throw new IOException("Column name is null");
    }
    for(String columnName: columnNames) {
      checkColumn(columnName);
    }
  }
  
  private void checkColumn(List<CellFilter> cellFilters) throws IOException {
    for(CellFilter cellFilter: cellFilters) {
      checkColumn(cellFilter.getColumnName());
    }
  }

  private void pause() throws IOException {
    try {
      Thread.sleep(pauseTime);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }
  
  private void startTx(Row.Key rowKey) throws IOException {
    startTx(rowKey, true);
  }
  
  private void startTx(Row.Key rowKey, boolean systemTimestamp) throws IOException {
    txOperation.startTx(rowKey, systemTimestamp);
  }
  
  /**
   * Set auto flush mode. If true, CTable doesn't flush immediately.
   * @param autoFlush
   */
  public void setAutoFlush(boolean autoFlush) {
    this.autoFlush = autoFlush;
  }
  
  /**
   * Return auto flush mode
   * @return
   */
  public boolean isAutoFlush() {
    return autoFlush;
  }
  
  /**
   * set buffer size. This value is valid when auto flush set true.
   * If bufferSize great than client.maxBufferSize, replaced maxBufferSize.
   * @param bufferSize
   */
  public void setBufferSize(int bufferSize) {
    if(bufferSize > maxBufferSize) {
     this.bufferSize = maxBufferSize;
    } else {
      this.bufferSize = bufferSize;
    }
  }
  
  /**
   * Put Row data in this table. 
   * @param row 
   * @throws IOException
   */
  public void put(Row row) throws IOException {
    put(row, true);
  }

  /**
   * Put Row data in this table. If the parameter <code>systemTimestamp</code> is set as true, 
   * Cloudata exploits system clock in TabletServer as an identifier of this Row object.
   * Otherwise, the application has to assign the timestamp value in Row object by itself.
   * In this case, the application must generate unique timestamp value to avoid data collisions.
   * @param row
   * @param systemTimestamp If true, use system timestamp. If false, use user timestamp and you must set timestamp in Cell.Value 
   * @throws IOException
   */
  public void put(Row row, boolean systemTimestamp) throws IOException {
    txOperation.startTx(row.getKey(), systemTimestamp);
    
    try {
      String[] columnNames = row.getColumnNames();
      if(columnNames == null) {
        return; 
      }
      
      for(String eachColumn: columnNames) {
        List<Cell> cells = row.getCellList(eachColumn);
        List<ColumnValue> eachColumnValues = new ArrayList<ColumnValue>();
        if(cells != null && cells.size() > 0) {
          for(Cell eachCell: cells) {
            for(Cell.Value eachValue: eachCell.getValues()) {
              ColumnValue columnValue = new ColumnValue();
              columnValue.setRowKey(row.getKey());
              columnValue.setCellKey(eachCell.getKey());
              eachValue.copyToColumnValue(columnValue);
              
              eachColumnValues.add(columnValue);
            }
          }
        }
        
        txOperation.insertColumnValue(eachColumn, eachColumnValues.toArray(new ColumnValue[]{}));
      }
      
      txOperation.commit(true);
    } catch (Exception e) {
      rollback();
      throw new IOException(e.getMessage(), e);
    }
  }
  
  /**
   * flush to TableServer
   * @throws IOException
   */
  public void flush() throws IOException {
    txOperation.flush();
  }
  
  private void commit() throws IOException {
    commit(true);
  }
  
  /**
   * saveLog값이 true인 경우 commit 처리를 하면서 commit log에 값을 추가한다.
   * 일반적인 경우는 무조건 true로 설정하고 테스트 용도로 false 값을 이용한다.
   * @param saveLog
   * @throws IOException
   */
  private void commit(boolean saveLog) throws IOException {
    txOperation.commit(saveLog);
  }
  
  private void rollback() throws IOException {
    txOperation.rollback();
  }
  
  /**
   * Remove all cells in a row  with system timestamp
   * @param rowKey
   * @throws IOException
   */
  public void removeRow(Row.Key rowKey) throws IOException {
    removeRow(rowKey, CommitLog.USE_SERVER_TIMESTAMP);
  }

  /**
   * Remove all cells in a row  with user timestamp
   * @param rowKey
   * @param timestamp When storing commit logs of this delete operation, use this timestamp value
   * @throws IOException
   */
  public void removeRow(Row.Key rowKey, long timestamp) throws IOException {
    startTx(rowKey);
    Row row = txOperation.removeRow(timestamp);
    if(row == null) {
      return;
    }
    commit();
    removeBlob(row);
  }
  
  /**
   * Remove a Cell in a Row 
   * @param rowKey
   * @param columnName
   * @param cellKey
   * @throws IOException
   */
  public void remove(Row.Key rowKey, String columnName, Cell.Key cellKey) throws IOException {
    BlobColumnCell blobColumnCell = null;
    if(tableSchema.isBlobType(columnName)) {
      byte[] fileInfoBytes = get(rowKey, columnName, cellKey);
      
      if(fileInfoBytes != null) {
        blobColumnCell = new BlobColumnCell(fileInfoBytes);
      }
    }
    
    startTx(rowKey);
    txOperation.deleteColumnValue(columnName, cellKey);
    commit();
    
    if(tableSchema.isBlobType(columnName)) {
      removeBlob(columnName, blobColumnCell);
    }
  } 

  /**
   * Check whether a Row with parameter <code>rowkey</code> exists or not
   * Instead, it is possible to use get() method, but it is not recommended due to performance reason 
   * @param columnName
   * @param rowKey
   * @param cellKey
   * @return returns true if there exists a row matching with <code>rowkey</code>, otherwise false
   * @throws IOException
   */
  public boolean hasValue(String columnName, Row.Key rowKey, Cell.Key cellKey) throws IOException {
    Exception exception = null;
    //TODO 데이터의 크기가 정해진 크기 이상인 경우 Exception이 발생한다.
    long startTime = System.currentTimeMillis();
    while(true) {
      TabletInfo tabletInfo = null;
      try {
        tabletInfo = lookupTargetTablet(rowKey);     
        if(tabletInfo != null) {
          DataServiceProtocol userTabletServer = CTableManager.connectTabletServer(tabletInfo, conf);
          return userTabletServer.hasValue(tabletInfo.getTabletName(), columnName, rowKey, cellKey);
        }
      } catch (Exception e) {
        if(tabletInfo != null) {
          //locationCache.clearAllCache();
          locationCache.clearTabletCache(tableName, rowKey, tabletInfo);
        }        
        exception = e;
      }
      if( (System.currentTimeMillis() - startTime) > txTimeout) {
        break;
      }
      pause();      
    }
    
    if(exception == null) {
      throw new IOException("fail get after " + txTimeout + " ms rowKey=" + rowKey);
    }  else {
      throw CTableManager.makeIOException(exception);
    }        
  }
  
  /**
   * Check whether a Row with parameter <code>rowkey</code> exists or not
   * Instead, it is possible to use get() method, but it is not recommended due to performance reason 
   * @param columnName
   * @param rowKey
   * @return returns true if there exists a row matching with <code>rowkey</code>, otherwise false
   * @throws IOException
   */
  public boolean hasValue(String columnName, Row.Key rowKey) throws IOException {
    return hasValue(columnName, rowKey, null);
  }
  
  /**
   * Retrieve data the application stored by <code>put</code> method 
   * @param rowKey
   * @param columnName
   * @param cellKey
   * @return null, if there is no data associating with parameters
   * @throws IOException
   */
  public byte[] get(Row.Key rowKey, String columnName, Cell.Key cellKey) throws IOException {
    RowFilter rowFilter = new RowFilter(rowKey);
    rowFilter.addCellFilter(new CellFilter(columnName, cellKey));
    
    Row row = get(rowFilter);
    
    if(row == null) {
      return null;
    }
    
    Cell cell = row.getFirst(columnName);
    if(cell == null) { 
      return null;
    }
    
    return cell.getValue().getBytes();
  }
  
  /**
   * Retrieve all the columns and their values in Row matching with <code>rowKey</code>
   * If the Row contains too much data, it may cause serious problem.
   * Therefore, Cloudata restricts the limit in transformation and IOException will be thrown 
   * if the limit exceeds. 
   * @param rowKey
   * @return null, If there is no data matching with the row key
   * @throws IOException
   */
  public Row get(Row.Key rowKey) throws IOException {
    RowFilter rowFilter = new RowFilter(rowKey);
    
    for(String eachColumn: tableSchema.getColumns()) {
      rowFilter.addCellFilter(new CellFilter(eachColumn));
    }
    return get(rowFilter);
  }

  /**
   * Retrieve data with row key and specific column names.
   * @param rowKey
   * @param columnNames
   * @return null, If there is no data matching with the row key
   * @throws IOException
   */
  public Row get(Row.Key rowKey, String[] columnNames) throws IOException {
    if(columnNames == null) {
      return null;
    }
    RowFilter rowFilter = new RowFilter(rowKey);
    
    for(String eachColumn: columnNames) {
      rowFilter.addCellFilter(new CellFilter(eachColumn));
    }
    return get(rowFilter);       
  }
  
  /**
   * This method allows applications to asynchronously receive data.
   * The invocation of this method is immediately returned after triggering data retrieval.
   * The result of retrieval is returned through DataReceiver specified in the parameter.
   * The retrieval is only valid within the timeout.
   * @param rowKeys
   * @param cellFilters
   * @param dataReceiver
   * @param numOfThread
   * @param timeout
   * @throws IOException
   */
  public boolean get(Collection<Row.Key> rowKeys, 
                    List<CellFilter> cellFilters, 
                    AsyncDataReceiver dataReceiver, 
                    int numOfThread, 
                    int timeout) throws IOException {
    checkColumn(cellFilters);
    
    ParallelGet parallelGet = new ParallelGet(this, rowKeys, cellFilters, numOfThread, timeout, dataReceiver);
    
    return parallelGet.get();
  }
  
  /**
   * This method allows applications to asynchronously receive data.
   * The invocation of this method is immediately returned after triggering data retrieval.
   * The result of retrieval is returned through DataReceiver specified in the parameter.
   * The retrieval is only valid within the timeout.
   * @param rowKeys
   * @param columnNames
   * @param dataReceiver
   * @parma numOfThread
   * @param timeout
   * @throws IOException
   */
  public boolean get(
      Collection<Row.Key> rowKeys, 
      String[] columnNames, 
      AsyncDataReceiver dataReceiver, 
      int numOfThread, 
      int timeout) throws IOException {
    List<CellFilter> columnFilters = new ArrayList<CellFilter>();
    
    for(String eachColumnName: columnNames) {
       columnFilters.add(new CellFilter(eachColumnName));
    }
    return get(rowKeys, columnFilters, dataReceiver, numOfThread, timeout);
  }
  
  /**
   * Retrieve data stored in Cloudata
   * @param rowKey
   * @param columnName
   * @return null, If there is no data matching with the row key
   * @throws IOException
   */
  public Row get(Row.Key rowKey, String columnName) throws IOException {
    return get(rowKey, new String[]{columnName});
  }
  
  /**
   * Retrieve data stored in Cloudata with a specific filter.
   * The filter contains the various conditions determining data to be retrieved to the client application
   * This filter is not applied to multiple rows
   * @param rowFilter
   * @return null, If there is no data matching with the row key
   * @throws IOException
   */
  public Row get(RowFilter rowFilter) throws IOException {
    if(rowFilter.getOperation() != RowFilter.OP_EQ) {
      throw new IOException("Not support multi row operation. use gets(RowFilter)");
    }
    rowFilter.checkFilterParam();
    
    if(rowFilter.getCellFilters().size() == 0) {
      throw new IOException("No CellFilter in RowFilter");
    }
    
    checkColumn(rowFilter.getColumns());
    Exception exception = null;
    
    long startTime = System.currentTimeMillis();
    while(true) {
      TabletInfo tabletInfo = null;
      try {
        tabletInfo = lookupTargetTablet(rowFilter.getStartRowKey());     
        if(tabletInfo != null) {
          DataServiceProtocol userTabletServer = CTableManager.connectTabletServer(tabletInfo, conf);
          RowColumnValues[] result = userTabletServer.get(tabletInfo.getTabletName(), rowFilter);
          return RowColumnValues.makeRow(result);
        }
      } catch (Exception e) {
        if(e instanceof PermissionException) {
          throw (PermissionException)e;
        }
        if(e instanceof GetFailException) {
          throw (GetFailException)e;
        }
        if(tabletInfo != null) {
          locationCache.clearTabletCache(tableName, rowFilter.getStartRowKey(), tabletInfo);
        }        
        exception = e;
      }
      if( (System.currentTimeMillis() - startTime) > txTimeout) {
        break;
      }
      pause();      
    }
    
    if(exception == null) {
      throw new IOException("Can't find Tablet for rowkey[" + rowFilter.getStartRowKey() + "]");
    }  else {
      throw CTableManager.makeIOException(exception);
    } 
  }
  
  /**
   * This method is different with other <code>get</code> methods in applying the operation 
   * to multiple rows. Also, convenient operations such as 'Like', 'Greater than' , 'Less than' 
   * are also supported.
   * @param rowFilter
   * @return null, If there is no data matching with the row key
   * @throws IOException
   */
  public Row[] gets(final RowFilter rowFilter) throws IOException {
    rowFilter.checkFilterParam();
    
    if(rowFilter.getCellFilters().size() == 0) {
      throw new IOException("No CellFilter in RowFilter");
    }
    checkColumn(rowFilter.getColumns());
    if(rowFilter.getOperation() == RowFilter.OP_EQ) {
      Row row = get(rowFilter);
      if(row == null) {
        return null;
      }
      
      return new Row[]{row};
    }
    
    if(rowFilter.getOperation() == RowFilter.OP_LIKE &&
        rowFilter.getStartRowKey().getLength() < minNumOfCharsForLikeOperation) {
      throw new IOException("[" + rowFilter.getStartRowKey() + "] too short rowkey for gets operation. must longer than conf.client.min.numOfChars.like value.");
    }
    
    List<Row> result = new ArrayList<Row>();
    
    Row.Key currentRowKey = rowFilter.getStartRowKey();
    boolean stop = false;
    
    Exception exception = null;
    
    long startTime = System.currentTimeMillis();
    
    int totalRowCount = 0;
    
    int maxRowNum = rowFilter.isPaging() ? rowFilter.getPageSize() : maxResultRecord; 
        
    while(!stop) {
      TabletInfo tabletInfo = null;
      RowColumnValues[][] rows = null;
      
      //하나의 Tablet에 대한 처리(정상적인 경우라면 1번만 수행된다)
      while(true) {
        try {
          tabletInfo = lookupTargetTablet(currentRowKey);
          if(tabletInfo != null) {
            DataServiceProtocol userTabletServer = CTableManager.connectTabletServer(tabletInfo, conf);
            rows = userTabletServer.gets(tabletInfo.getTabletName(), currentRowKey, rowFilter);
            break;
          }
        } catch (Exception e) {
          if(e instanceof PermissionException) {
            throw (PermissionException)e;
          }
          if(e instanceof GetFailException) {
            throw (GetFailException)e;
          }
          LOG.debug("Error while gets:" + e.getMessage() + ", but retry");
          if(tabletInfo != null) {
            locationCache.clearTabletCache(tableName, currentRowKey, tabletInfo);
          }        
          exception = e;
        }
        
        //timeout 시간 이상인 경우
        if( (System.currentTimeMillis() - startTime) > txTimeout * 3) {
          if(exception == null) {
            throw new IOException("fail to gets after " + txTimeout * 3 + " ms");
          }  else {
            throw CTableManager.makeIOException(exception);
          } 
        }
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      }
      
      //조회된 결과를 Row로 변환하여 return value에 추가
      if(rows != null && rows.length > 0) {
        for(int i = 0; i < rows.length; i++) {
          Row row = RowColumnValues.makeRow(rows[i]);
          if(row != null) {
            result.add(row);
            totalRowCount++;
          }
          if(totalRowCount >= maxRowNum) {
            stop = true;
            break;
          }
        }
      }
      
      if(tabletInfo.getEndRowKey().compareTo(rowFilter.getEndRowKey()) >= 0) {
        stop = true;
        break;
      }
      
//      byte[] tabletEndRowKeyBytes = tabletInfo.getEndRowKey().getBytes();
//      byte[] startKeyBytes = new byte[tabletEndRowKeyBytes.length + 1];
//      System.arraycopy(tabletEndRowKeyBytes, 0, startKeyBytes, 0, tabletEndRowKeyBytes.length);
//      startKeyBytes[tabletEndRowKeyBytes.length] = Byte.MIN_VALUE;
//      currentRowKey = new Row.Key(startKeyBytes);
      currentRowKey = new Row.Key(tabletInfo.getEndRowKey().nextKeyBytes());
    }
    
    if(result.size() == 0) {
      return null;
    }
    return result.toArray(new Row[]{});
  }
  
  /**
   * Return RowIterator object. RowIterator calls CTable.gets() method internally
   *  RowIterator doesn't support remove() method. 
   * @param rowFilter
   * @return
   * @throws IOException
   */
  public Iterator<Row> iterator(RowFilter rowFilter) throws IOException {
    return new RowIterator(this, rowFilter);
  }
  
  /**
   * Create a new table according to the table schema information
   * @param conf Configuration variables of Cloudata
   * @param tableSchema
   * @throws IOException
   */
  public static void createTable(CloudataConf conf, TableSchema tableSchema) throws IOException {
    createTable(conf, tableSchema, null);
  }
  
  /**
   * Create a new table according to the table schma information, then split 
   * the newly created table into multiple tablets by the number of row keys specified 
   * in the parameter. Each row key value represents the end row key value of each divided tablet.
   * @param endRowKeys
   * @throws IOException
   */
  public static void createTable(CloudataConf conf, TableSchema tableSchema, Row.Key[] endRowKeys) throws IOException {
    if(endRowKeys != null && endRowKeys.length > 5000) {
      throw new IOException("Too many tablets:" + endRowKeys.length + ", limits is " + 5000);
    }
    setUserId(conf);
    if(tableSchema == null) {
      throw new IOException("Table create error: TableSchema is null");
    }
    
    if(tableSchema.getTableName() == null || tableSchema.getTableName().trim().length() == 0 ||
        !tableSchema.getTableName().matches(TableSchema.TABLE_NAME_PATTERN)) {
      throw new IOException("Table create error: Wrong table name:" + tableSchema.getTableName());
    }
    
    List<ColumnInfo> columns = tableSchema.getColumnInfos();
    
    if(columns == null || columns.size() == 0) {
      throw new IOException("Table create error: No columns in table");
    }
    
    tableSchema.setOwner(conf.getUserId());
    CTableManager.createTable(conf, tableSchema, endRowKeys);
    
    int tabletCount = endRowKeys == null ? 1 : endRowKeys.length;
    long startTime = System.currentTimeMillis();
    while(true) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
      
      try {
        TabletInfo[] tabletInfos = CTable.listTabletInfos(conf, tableSchema.getTableName());
        if(tabletInfos != null && tabletInfos.length >= tabletCount) {
          break;
        }
      } catch (PermissionException e) {
        throw e;
      } catch (Exception e) {
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e1) {
          LOG.error(e1.getMessage());
          return;
        }
      }
      
      //1 min
      if(System.currentTimeMillis() - startTime >= 60 * 1000 ) {
        throw new IOException("Timeout while createTable:" + tableSchema.getTableName());
      }
    }
    
    for(ColumnInfo eachColumn: columns) {
      if(eachColumn.getColumnType() == TableSchema.BLOB_TYPE) {
        createBlobTable(conf, tableSchema, eachColumn.getColumnName());
      }
    }
  }
  
  private static void createBlobTable(CloudataConf conf, TableSchema tableSchema, String columnName) throws IOException {
    TableSchema blobTableSchema = new TableSchema(BlobMetaManager.getBlobTableName(tableSchema.getTableName(), columnName));
    blobTableSchema.addColumn(new ColumnInfo(Constants.BLOB_COLUMN_FILE));
    blobTableSchema.addColumn(new ColumnInfo(Constants.BLOB_COLUMN_DELETE));
    
    blobTableSchema.setOwner(tableSchema.getOwner());
    for(Map.Entry<String, String> entry: tableSchema.getPermissions().entrySet()) {
      blobTableSchema.addPermission(entry.getKey(), entry.getValue());
    }
    createTable(conf, blobTableSchema, null);
  }
  
  /**
   * Drop the table specified by the tableName parameter
   * @param conf Configuration variables of Cloudata
   * @param tableName table name
   * @throws IOException
   */
  public static void dropTable(final CloudataConf conf, final String tableName) throws IOException {
    setUserId(conf);
    
    if(!CTable.existsTable(conf, tableName)) {
      LOG.debug("No table:" + tableName);
      return;
    }
    
    CTable ctable = CTable.openTable(conf, tableName);
    TableSchema tableSchema = ctable.descTable();
    
    final TableManagerProtocol masterServer = CTableManager.getMasterServer(conf);
    
    AsyncCall asyncCall = new AsyncCall() {
      public String run() throws IOException{
        return masterServer.dropTable(tableName);
      }
    };
    asyncCall.call("drop table " + tableName, masterServer, 600 * 1000);
    
    //Cache에서 삭제
    TabletLocationCache.getInstance(conf).removeTableSchemaCache(tableName);
    
    //Blob table 삭제
    for(ColumnInfo eachColumn: tableSchema.getColumnInfos()) {
      if(eachColumn.getColumnType() == TableSchema.BLOB_TYPE) {
        dropBlobTable(conf, tableName, eachColumn.getColumnName());
      }
    }
  }

  private static void dropBlobTable(final CloudataConf conf,
      final String tableName, String columnName) throws IOException {
    String blobTableName = BlobMetaManager.getBlobTableName(tableName, columnName);
    dropTable(conf, blobTableName);
    CloudataFileSystem fs = CloudataFileSystem.get(conf);
    fs.delete(new GPath(BlobMetaManager.getBlobRootPath(conf, tableName, columnName)), true);
    fs.delete(new GPath(BlobMetaManager.getBlobTempPath(conf, tableName, columnName)), true);
  }
  
  /**
   * Drop the table of this CTable instance.
   * All the data and files associated with the table are removed.
   * @throws IOException
   */
  public void dropTable() throws IOException {
    CTable.dropTable(conf, tableName);
  }

  /**
   * Delete all the data contained in this table.
   * @param clearPartitionInfo clear partition information if true.
   * @throws IOException
   */
  public void truncateTable(boolean clearPartitionInfo) throws IOException {
    TabletInfo[] tabletInfos = listTabletInfos();
    if(tabletInfos == null || tabletInfos.length == 0) {
      return;
    }
    
    dropTable();
    
    try {
      Thread.sleep(2 * 1000);
    } catch (InterruptedException e) {
      return;
    } 
    
    if(clearPartitionInfo) {
      CTable.createTable(conf, tableSchema);
    } else {
      Row.Key[] rowKeys = new Row.Key[tabletInfos.length];
      
      int i = 0;
      for(TabletInfo eachTabletInfo: tabletInfos) {
        rowKeys[i++] = eachTabletInfo.getEndRowKey();
      }
      
      CTable.createTable(conf, tableSchema, rowKeys);
    }
    
    try {
      Thread.sleep(5 * 1000);
    } catch (InterruptedException e) {
      return;
    }
  }
  
  /**
   * Delete all the data contained in this table.
   * @throws IOException
   */
  public void truncateTable() throws IOException {
    truncateTable(true);
  }
  
  /**
   * Delete all the data of a column specified by <code>columnName</code> in this table.
   * @param columnName
   * @throws IOException
   */
  public void truncateColumn(String columnName) throws IOException {
    checkColumn(columnName);
    TabletInfo[] tabletInfos = listTabletInfos();
    if(tabletInfos == null || tabletInfos.length == 0) {
      return;
    }
    
    for(TabletInfo eachTabletInfo: tabletInfos) {
      try {
        CTableManager.connectTabletManager(eachTabletInfo.getAssignedHostName(), conf).truncateColumn(eachTabletInfo.getTabletName(), columnName);
        
        if(tableSchema.isBlobType(columnName)) {
          dropBlobTable(conf, tableName, columnName);
          
          createBlobTable(conf, tableSchema, columnName);
        }
      } catch (IOException e) {
        LOG.warn("Error while truncate table:" + eachTabletInfo + "," + e.getMessage());
      }
    }
    
    try {
      Thread.sleep(5 * 1000);
    } catch (InterruptedException e) {
      return;
    }
  }

  /**
   * Retrieve table schema information of this CTable instance
   * @return If no tablet schema information, returns null
   * @throws IOException
   */
  public TableSchema descTable() throws IOException {
	/*
    AclManager.checkPermission(conf, 
        CTableManager.getZooKeeper(conf),
        conf.getUserId(),
        locationCache.getTableInfos(), tableName, "r");
    */
    tableSchema = locationCache.getTableSchema(tableName);
    return tableSchema;
  }
  
  /**
   * Append new column. There will be no changes of data contained by this table
   * @param addedColumnName
   * @throws IOException
   */
  
  public void addColumn(String addedColumnName) throws IOException {
    CTableManager.getMasterServer(conf).addColumn(tableName, addedColumnName);
    
    //retry
    for(int i = 0; i < 5; i++) {
      TabletInfo[] tabletInfos = listTabletInfos();
      if(tabletInfos == null || tabletInfos.length == 0) {
        continue;
      }
      
      //FIXME: 속도 문제와 split 중인 경우에 대한 처리 등
      for(TabletInfo eachTabletInfo: tabletInfos) {
        CTableManager.connectTabletManager(eachTabletInfo.getAssignedHostName(), conf).
                              addColumn(tableName, eachTabletInfo.getTabletName(), addedColumnName);
      }
    }
    tableSchema = locationCache.reloadTableSchema(tableName);
  }
  
  
  /**
   * Add new tablet in this table with the specified row key
   * @param tableName table name
   * @param endRowKey
   * @return
   * @throws IOException
   */
  public TabletInfo addTablet(Row.Key startRowKey, Row.Key endRowKey) throws IOException {
    Row.Key metaRowKey = Tablet.generateMetaRowKey(tableName, endRowKey);
    CTable ctable = CTable.openTable(conf, TABLE_NAME_META);
    if(ctable.hasValue(META_COLUMN_NAME_TABLETINFO, metaRowKey)) {
      Row row = ctable.get(metaRowKey, META_COLUMN_NAME_TABLETINFO);
      if(row != null) {
        TabletInfo tabletInfo = new TabletInfo();
        tabletInfo.readFields(row.getFirst(META_COLUMN_NAME_TABLETINFO).getBytes());
        return tabletInfo;
      }
    }
    
    TableManagerProtocol masterServer = CTableManager.getMasterServer(conf);
    
    try {
      TabletInfo tabletInfo = masterServer.addTablet(tableName, startRowKey, endRowKey);
      //Tablet이 할당될때까지 기다린다.
      int retry = 0;
      while(retry < 30) {
        try {
          Thread.sleep(2 * 1000);
        } catch (InterruptedException e1) {
          return null;
        }
        if(ctable.hasValue(META_COLUMN_NAME_TABLETINFO, metaRowKey)) {
          Row row = ctable.get(metaRowKey, META_COLUMN_NAME_TABLETINFO);
          TabletInfo tabletInfo2 = new TabletInfo();
          tabletInfo2.readFields(row.getFirst(META_COLUMN_NAME_TABLETINFO).getBytes());
          if(tabletInfo2.getTabletName().equals(tabletInfo.getTabletName())) {
            return tabletInfo2;
          }
        }
        retry++;
      }
      return tabletInfo;
    } catch (IOException e) {
      int retry = 0;
      while(retry < 10) {
        try {
          Thread.sleep(5 * 1000);
        } catch (InterruptedException e1) {
          return null;
        }
        if(ctable.hasValue(META_COLUMN_NAME_TABLETINFO, metaRowKey)) {
          Row row = ctable.get(metaRowKey, META_COLUMN_NAME_TABLETINFO);
          TabletInfo tabletInfo = new TabletInfo();
          tabletInfo.readFields(row.getFirst(META_COLUMN_NAME_TABLETINFO).getBytes());
          return tabletInfo;
        }
        retry++;
      }
      
      throw e;
    }
  }
  
  /**
   * Get the list of all the tablet information of this table.
   * @return 
   * @throws IOException
   */
  public TabletInfo[] listTabletInfos() throws IOException {
    return CTable.listTabletInfos(conf, tableName);
  }

  private static TabletInfo[] listTabletInfos(CloudataConf conf, String tableName) throws IOException {
    setUserId(conf);
    //FIXME 속도 테스트 필요
    List<TabletInfo> tabletInfos = new ArrayList<TabletInfo>();
    
    //ROOT Table인 경우
    if(TABLE_NAME_ROOT.equals(tableName)) {
      return new TabletInfo[]{Tablet.getRootTabletInfo(conf,  TabletLocationCache.getInstance(conf).getZooKeeper())};
    }
    
    TableScanner rootScanner = null;
    
    try {
      CTable rootTable = CTable.openTable(conf, TABLE_NAME_ROOT);
      CTable metaTable = CTable.openTable(conf, TABLE_NAME_META);
      rootScanner = ScannerFactory.openScanner(rootTable.getConf(), 
          Tablet.getRootTabletInfo(conf, TabletLocationCache.getInstance(conf).getZooKeeper()), 
          META_COLUMN_NAME_TABLETINFO);
    
/*  
      ScanCell rootCell = null;
      while( (rootCell = rootScanner.next()) != null) {
        TabletInfo metaTabletInfo = new TabletInfo();
        metaTabletInfo.readFields(rootCell.getBytes());
        
        if(TABLE_NAME_META.equals(tableName)) {
          //META table인 경우
          tabletInfos.add(metaTabletInfo);
          //LOG.debug("add tablet:" + metaTabletInfo);
        } else {
          //일반 table인 경우 META Scanner를 open한다.
          //TableScanner metaScanner = new MultiTabletScanner(metaTable, META_COLUMN_NAME_TABLETINFO);
          TableScanner metaScanner = ScannerFactory.openScanner(metaTable, META_COLUMN_NAME_TABLETINFO); 
          try {
            ScanCell metaCell = null;
            while( (metaCell = metaScanner.next()) != null ) {
              TabletInfo userTabletInfo = new TabletInfo();
              userTabletInfo.readFields(metaCell.getBytes());
*/
      Row rootRow = null;
      while( (rootRow = rootScanner.nextRow()) != null) {
        TabletInfo metaTabletInfo = new TabletInfo();
        List<Cell> cells = rootRow.getCellList(META_COLUMN_NAME_TABLETINFO); 
        metaTabletInfo.readFields(cells.get(cells.size() - 1).getBytes());
        
        if(TABLE_NAME_META.equals(tableName)) {
          //META table인 경우
          tabletInfos.add(metaTabletInfo);
          //LOG.debug("add tablet:" + metaTabletInfo);
        } else {
          //일반 table인 경우 META Scanner를 open한다.
          //TableScanner metaScanner = new MultiTabletScanner(metaTable, META_COLUMN_NAME_TABLETINFO);
          TableScanner metaScanner = ScannerFactory.openScanner(metaTable, META_COLUMN_NAME_TABLETINFO); 
          try {
            //ScanCell metaCell = null;
            Row metaRow = null;
            while( (metaRow = metaScanner.nextRow()) != null ) {
              TabletInfo userTabletInfo = new TabletInfo();
              cells = metaRow.getCellList(META_COLUMN_NAME_TABLETINFO); 
              userTabletInfo.readFields(cells.get(cells.size() - 1).getBytes());

              //System.out.println("userTabletInfo:" + userTabletInfo);
              if(userTabletInfo.getTableName().compareTo(tableName) > 0) {
                break;
              }
              if(tableName.equals(userTabletInfo.getTableName())) {
                tabletInfos.add(userTabletInfo);
                //LOG.debug("add tablet:" + userTabletInfo);
              }
            }
          } finally {
            if(metaScanner != null)  { try { metaScanner.close(); } catch (Exception e) {} }
          }
        }
      }
    } catch (IOException e) {
      throw e;
    } finally {
      if(rootScanner != null)   { try { rootScanner.close(); } catch (Exception e) {} }
    }    
    
    if(tabletInfos.size() == 0) {
      //LOG.info("listTabletInfos for table[" + tableName + "] is null");
      return null;
    }
    return tabletInfos.toArray(new TabletInfo[tabletInfos.size()]);
  }
  
  /**
   * Open new SortedDirectUploader and return its instance. 
   * @param columnNames
   * @return BatchUploader instance
   * @throws IOException
   */
  public DirectUploader openDirectUploader(String[] columnNames) throws IOException {
    checkColumn(columnNames);
    return openDirectUploader(columnNames, false);
  }
  
  public DirectUploader openDirectUploader(String[] columnNames, boolean inputDataSorted) throws IOException {
    checkColumn(columnNames);
    if(inputDataSorted) {
      return new DefaultDirectUploader(this, columnNames);
    } else {
      return new SortDirectUploader(this, columnNames);
    }
  }
  
  /**
   * (Only for management) Retrieve the list of all the actions internally performed in specified tablet
   * @param tabletInfo
   * @return TabletAction의 class명
   * @throws IOException
   */
  public String[] getTabletAllActions(TabletInfo tabletInfo) throws IOException {
    return CTableManager.connectTabletServer(tabletInfo, conf).getAllActions(tabletInfo.getTabletName());
  }
  
  /**
   * (Only for management) Get the report of tablet, describing current status of specified tablet.
   * @param tabletInfo
   * @return TabletReport 
   * @throws IOException
   */
  public TabletReport getTabletReport(TabletInfo tabletInfo) throws IOException {
    if(tabletInfo.getAssignedHostName() == null) {
      return null;
    }
    return CTableManager.connectTabletServer(tabletInfo, conf).getTabletDetailInfo(tabletInfo.getTabletName());
  }


  /**
   * Find a tablet serving a specified row and return its information
   * @param rowKey
   * @return tablet information
   * @throws IOException
   */
  public TabletInfo lookupTargetTablet(Row.Key rowKey) throws IOException {
    //NStopWatch watch = new NStopWatch();
    //watch.start("lookupTargetTablet", 5);
    TabletInfo tabletInfo = lookupTargetTablet(conf, tableName, rowKey);
    //watch.stopAndReportIfExceed(LOG);
    return tabletInfo;
  }
  /**
   * Find a tablet serving a specified row and return its information
   * @param hashRow.Key
   * @return tablet information
   * @throws IOException
   */
  public static TabletInfo lookupTargetTablet(CloudataConf conf, String tableName, Row.Key rowKey) throws IOException {
    setUserId(conf);
    if(TABLE_NAME_ROOT.equals(tableName)) {
      return TabletLocationCache.getInstance(conf).lookupRootTablet();
    } else  if(TABLE_NAME_META.equals(tableName)) {
      return TabletLocationCache.getInstance(conf).lookupMetaTablet(tableName, rowKey);
    } else {
      return TabletLocationCache.getInstance(conf).lookupUserTablet(tableName, rowKey);
    }
  } 
  
  class TxOperation {
    protected TxId txId;
    protected DataServiceProtocol userTabletServer;
    protected TabletInfo tabletInfo;
    protected Row.Key rowKey;
    
    protected List<CommitLog> commitLogList = new ArrayList<CommitLog>(10);
    
    ApplyCallWithoutReturn applyCall = new ApplyCallWithoutReturn();
    
    public void insertColumnValue(String columnName, ColumnValue[] values) throws IOException {
      checkColumn(columnName);
      boolean useSystemTimestamp = txId.isSystemTimestamp();
      
      for(ColumnValue record : values) {
        internalInsertColumnValue(columnName, record, useSystemTimestamp ? CommitLog.USE_SERVER_TIMESTAMP : record.getTimestamp());
      }
    }

    public Row deleteColumnValue(String columnName, Cell.Key cellKey) throws IOException {
      checkColumn(columnName);
      
      if(cellKey == null || cellKey.equals(Cell.Key.EMPTY_KEY) || cellKey.isEmpty()) {
        Row row = CTable.this.get(rowKey, columnName);
        List<Cell> cells = row.getCellList(columnName);
        if(cells != null) {
          for(Cell eachCell: cells) {
            internalDeleteColumnValue(columnName, eachCell.getKey(), CommitLog.USE_SERVER_TIMESTAMP);
          }
        }
        
        return row;
      } else {
        internalDeleteColumnValue(columnName, cellKey, CommitLog.USE_SERVER_TIMESTAMP);
        return null;
      }
    }
    
    public void updateColumnValue(String columnName, ColumnValue[] records) throws IOException {
      checkColumn(columnName);
      
      for(ColumnValue value : records) {
        long timestamp = txId.isSystemTimestamp() ? CommitLog.USE_SERVER_TIMESTAMP : value.getTimestamp();
        internalDeleteColumnValue(columnName, value.getCellKey(), timestamp);
        internalInsertColumnValue(columnName, value, timestamp);
      }
    }
    
    public Row removeRow(long timestamp) throws IOException {
      String[] columnNames = CTable.this.tableSchema.getColumnsArray();
      
      Row row = CTable.this.get(rowKey);
      
      if(row == null) {
        LOG.info("No data to remove:" + rowKey);
        return null;
      }
      for(String columnName : columnNames) {
        List<Cell> cells = row.getCellList(columnName);
        if(cells == null) {
          continue;
        }
        for(Cell eachCell: cells) {
          internalDeleteColumnValue(columnName, eachCell.getKey(), timestamp);
        }
      }
      
      return row;
    }
    
    public void flush() throws IOException {
      try {
        FlushCallWithoutReturn flushCall = new FlushCallWithoutReturn();
        flushCall.call();
      } catch (IOException e) {
        LOG.error("Commit error:" + this.rowKey + "," + this.tabletInfo, e);
        locationCache.clearAllCache();
        throw e;
      } finally {
        this.txId = null;
        this.rowKey = null;
        this.tabletInfo = null;
        bufferWriteSize = 0;
        commitLogList.clear();
      }
    }
    
    public void rollback() throws IOException {
      txId = null;
      rowKey = null;
      commitLogList.clear();
    }

    private void internalDeleteColumnValue(String columnName,
        Cell.Key columnKey, long timestamp) throws IOException {
      CommitLog commitLog = new CommitLog(Constants.LOG_OP_DELETE_COLUMN_VALUE, 
          tabletInfo.getTableName(), 
          rowKey,
          columnName,
          columnKey,
          timestamp,
          null);   
      commitLogList.add(commitLog);
    }

    private void internalInsertColumnValue(String columnName, ColumnValue record, long timestamp) 
        throws IOException {
      CommitLog commitLog = new CommitLog(Constants.LOG_OP_ADD_COLUMN_VALUE, 
        tableName, 
        record.getRowKey(),
        columnName,
        record.getCellKey(),
        timestamp,
        record.getValue());

      commitLogList.add(commitLog);
      
      //FIXME replace memory size
      bufferWriteSize += commitLog.getByteSize();
    }   

    public void startTx(Row.Key rowKey, boolean useSystemTimestamp) throws IOException {
      if(!autoFlush) {
        this.txId = TxId.generate("FLUSH_MODE", useSystemTimestamp);
        return;
      }
      this.rowKey = rowKey;
      
      long startTime = System.currentTimeMillis();
      
      while(true) {
        try {
          this.tabletInfo = lookupTargetTablet(rowKey);
          if(tabletInfo != null) {
            this.userTabletServer = CTableManager.connectTabletServer(tabletInfo, conf);
          }
          
          if (this.tabletInfo != null && this.userTabletServer != null) {
            break;
          }
          
          locationCache.clearTabletCache(tableName, rowKey, tabletInfo);
  
          if( (System.currentTimeMillis() - startTime) > txTimeout / 2) {
            locationCache.clearAllCache();
          }
        } catch (IOException e) {
          LOG.warn("startTx error:" + e.getMessage() + ", but retry");
        }
        if( (System.currentTimeMillis() - startTime) > txTimeout) {
          throw new IOException("Cannot start transaction rowkey [" 
              + rowKey + "] due to failure of getting tablet info for " + txTimeout + "ms");
        }

        pause();
      }

      this.txId = TxId.generate(tabletInfo.getTabletName(), useSystemTimestamp);
    }

    public void commit(final boolean saveLog) throws IOException {
      if(!autoFlush) {
        if(bufferWriteSize > bufferSize) {
          try {
            FlushCallWithoutReturn flushCall = new FlushCallWithoutReturn();
            flushCall.call();
          } catch (IOException e) {
            LOG.error("Flush error:" + e.getMessage(), e);
            locationCache.clearAllCache();
            throw e;
          } finally {
            this.txId = null;
            this.rowKey = null;
            this.tabletInfo = null;
            this.commitLogList.clear();
            bufferWriteSize = 0;
          }
        }
      } else {
        try {
          applyCall.call();
        } catch (IOException e) {
          LOG.error("Commit error:" + this.rowKey + "," + this.tabletInfo, e);
          locationCache.clearAllCache();
          throw e;
        } finally {
          this.txId = null;
          this.rowKey = null;
          this.tabletInfo = null;
  
          this.commitLogList.clear();
        }
      }
    }

    class ApplyCallWithoutReturn extends RetryCallWithoutReturn {
      int cacheClearCount = 0;
      
      ApplyCallWithoutReturn() {
        super(conf);
      }
      
      void run(RetryFlag flag) throws IOException {
        if (userTabletServer == null || tabletInfo == null) {
          flag.retry = true;
          flag.reloadMeta = true;
          return;
        }
        long sleepTime = userTabletServer.apply(
            tabletInfo.getTabletName(),
            rowKey, 
            txId, 
            commitLogList.toArray(new CommitLog[0]), 
            true);
        
        flag.retry = (sleepTime > 0);
        flag.sleep = sleepTime;
        if(flag.retry) {
          flag.reloadMeta = true;
        }
      }

      protected void executeRetryHandler(long remainedTxTime, boolean reloadMeta) throws IOException {
        if (!reloadMeta) {
          return;
        }
        
        long startTime = System.currentTimeMillis();
        while(remainedTxTime > (System.currentTimeMillis() - startTime)) {
          cacheClearCount++;
          try {
            /*
            if(cacheClearCount > 5) {
              locationCache.clearTabletCache(tableName, tabletInfo);
              cacheClearCount = 0;
            }
            locationCache.clearTabletCache(tableName, rowKey, tabletInfo);
            */
            locationCache.clearAllCache();
            tabletInfo = lookupTargetTablet(rowKey);
  
            if (tabletInfo != null) {
              if ((userTabletServer = CTableManager.connectTabletServer(tabletInfo, conf)) != null) {
                break;
              }
            }
          } catch (Exception e) {
            LOG.warn("executeRetryHandler error:" + e.getMessage() + ", but retry");
          }

          try { Thread.sleep(1000); } catch(Exception e) { return; }
        }

        if (tabletInfo == null || userTabletServer == null) {
          throw new IOException("Cannot lookup tablet info for the transaction timed out:" + rowKey);
        }
      }
    }    
    
    class FlushCallWithoutReturn extends RetryCallWithoutReturn {
      int currentIndex;
      
      boolean locked = false;
      
      CloudataConf conf;
      
      List<CommitLog> retryTargets = new ArrayList<CommitLog>();
      
      FlushCallWithoutReturn() {
        super(CTable.this.conf);
        
        this.conf = new CloudataConf(CTable.this.conf);
        this.conf.set("ipc.client.timeout", 30 * 1000);
        
        Collections.sort(commitLogList);
      }
      
      private boolean initTabletInfo(Row.Key rowKey) throws IOException {
        tabletInfo = lookupTargetTablet(rowKey);
        if(tabletInfo == null) {
          tabletInfo = lookupTargetTablet(rowKey);
          if(tabletInfo == null) {
            return false;
          }
        }
        
        userTabletServer = CTableManager.connectTabletServer(tabletInfo, this.conf);
        if(userTabletServer == null) {
          return false;
        }
        
        txId = TxId.generate(tabletInfo.getTabletName(), true);

        return true;
      }

      boolean flush(List<CommitLog> targetCommitLogs, RetryFlag flag) throws IOException {
        boolean result = false;
        try {
          //set server split lock
          if(!userTabletServer.startFlush(tabletInfo.getTabletName(), txId)) {
            LOG.info("Can't start flush:" + tabletInfo.getTabletName() + "," + tabletInfo.getAssignedHostName());
            flag.sleep = 50;
            return false;
          }
          locked = true;
          
          long startTime = System.currentTimeMillis();
          long sleepTime = userTabletServer.apply(
              tabletInfo.getTabletName(),
              targetCommitLogs.get(0).getRowKey(), 
              txId, 
              targetCommitLogs.toArray(new CommitLog[0]), 
              false);
          if(sleepTime > 0) {
            LOG.info("Can't apply:" + tabletInfo.getTabletName() + "," + tabletInfo.getAssignedHostName() + ", targetCommitLogs.size()=" + targetCommitLogs.size());
            flag.sleep = sleepTime;
          }
          long endTime = System.currentTimeMillis();
          if((endTime - startTime) > 2000) {
            LOG.info("Too long time:apply:" + tabletInfo.getTabletName() + ", time="  + (endTime - startTime));
          }
        } catch (Exception e) {
          result = false;
        } 
        
        try {
          userTabletServer.endFlush(tabletInfo.getTabletName(), txId);
        } finally {
          locked = false;
        }
        return result;
      }
      
      void run(RetryFlag flag) throws IOException {
        runInternal(flag);
        if(retryTargets.size() > 0) {
          if(currentIndex == commitLogList.size()) {
            commitLogList = retryTargets;
            retryTargets = new ArrayList<CommitLog>();
            currentIndex = 0;
            flag.retry = true;
            
            return;
          }
        }
      }
      
      void runInternal(RetryFlag flag) throws IOException {
        if(commitLogList.size() == 0 || currentIndex >= commitLogList.size()) {
          flag.retry = false;
          return;
        }
        
        List<CommitLog> targets = new ArrayList<CommitLog>();
        
        CommitLog commitLog = commitLogList.get(currentIndex);
        if(!initTabletInfo(commitLog.getRowKey())) {
          flag.retry = true;
          return;
        }
        
        targets.add(commitLog);
        
        Row.Key tabletEndRowKey = tabletInfo.getEndRowKey();

        int size = commitLogList.size();
        for(int i = currentIndex + 1; i < size; i++) {
          commitLog = commitLogList.get(i);
          if(commitLog.getRowKey().compareTo(tabletEndRowKey) > 0) {
            if(!flush(targets, flag)) {
              retryTargets.addAll(targets);
            }
            currentIndex = i;
            targets.clear();
            
            if(!initTabletInfo(commitLog.getRowKey())) {
              flag.retry = true;
              return;
            }
            tabletEndRowKey = tabletInfo.getEndRowKey();
          } 
          targets.add(commitLog);
        }
        
        if(targets.size() > 0) {
          if(!flush(targets, flag)) {
            retryTargets.addAll(targets);
          }
          targets.clear();
        }
        currentIndex = commitLogList.size();
        flag.retry = false;
      }

      protected void executeRetryHandler(long remainedTxTime) throws IOException {
        try {
          if(locked && tabletInfo != null && userTabletServer != null) {
            try {
              userTabletServer.endFlush(tabletInfo.getTabletName(), txId);
            } finally {
              locked = false;
            }
          }
          if(tabletInfo != null) {
            locationCache.clearTabletCache(tableName, tabletInfo);
          }
        } catch (Exception e) {
          locationCache.clearAllCache();
          LOG.warn("executeRetryHandler error:" + e.getMessage() + ", but retry");
        }
      }
    } 
  }

  /**
   * Return current table name.
   * @return
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * Return Cloudata configuration object.
   * @return 
   */
  public CloudataConf getConf() {
    return conf;
  }

  /**
   * Return column array
   * @return
   */
  public String[] getColumnsArray() throws IOException {
    TableSchema tableSchema = descTable();
    if(tableSchema == null) {
      return null;
    }
    return tableSchema.getColumnsArray();
  }

  /**
   * Return the tablet information
   * @param tabletName
   * @return
   * @throws IOException
   */
  public TabletInfo getTabletInfo(String tabletName) throws IOException {
    if(tabletName == null) {
      return null;
    }
    
    //FIXME Tablet이 많은 경우 속도 문제 해결
    Exception exception = null;
    long startTime = System.currentTimeMillis();
    while(true) {           
      try {
        TabletInfo[] tabletInfos = this.listTabletInfos();
        if(tabletInfos != null) {
          for(TabletInfo eachTabletInfo: tabletInfos) {
            if(tabletName.equals(eachTabletInfo.getTabletName())) {
              return eachTabletInfo;
            }
          }
        }
      } catch (IOException e) {
        exception = e;
      }
      if( (System.currentTimeMillis() - startTime) > txTimeout) {
        break;
      }
      pause();      
    }
    
    if(exception == null) {
      throw new IOException("fail getTabletInfo after " + txTimeout + " ms tabletName=" + tabletName);
    }  else {
      throw CTableManager.makeIOException(exception);
    } 
  }

  /**
   * Return the table schema information describing this table.
   * @deprecated
   * @return
   */
  public TableSchema getTableSchema() {
    return tableSchema;
  } 
  
  /**
   * Divides row keys contained in a certain tablet into <code>splitPerTablet</code> rows.
   * This method is useful in MapReduce application. When multiple Map tasks perform their job 
   * from one tablet on MapReduce, the rows in the tablet should be distributed to the Map tasks. 
   * This method helps divide the range of rows to assign multiple Map tasks.    
   * @param tabletInfo
   * @param splitPerTablet
   * @return
   * @throws IOException
   */
  public Row.Key[] getSplitedRowKeyRanges(TabletInfo tabletInfo, int splitPerTablet) throws IOException {
    DataServiceProtocol tabletServer = CTableManager.connectTabletServer(tabletInfo, conf);
    return tabletServer.getSplitedRowKeyRanges(tabletInfo.getTabletName(), splitPerTablet);
  }
  
  public String toString() {
    if(tableName == null) {
      return "CTable[No table name]";
    } else {
      return "CTable[" + tableName + "]";
    }
  }
  
  public OutputStream createBlob(Row.Key rowKey, String columnName, Cell.Key cellKey) throws IOException {
    if(!tableSchema.isBlobType(columnName)) {
      throw new IOException("Not blob type:" + columnName);
    }

    CBlobOutputStream out = new CBlobOutputStream(conf, new BlobMetaCell(tableName, rowKey, columnName, cellKey));
    
    return out;
  }

  public InputStream openBlob(Row.Key rowKey, String columnName, Cell.Key cellKey) throws IOException {
    if(!tableSchema.isBlobType(columnName)) {
      throw new IOException("Not blob type:" + columnName);
    }

    return new NBlobInputStream(this, rowKey, columnName, cellKey);
  }
  
  private void removeBlob(Row row) throws IOException {
    if(row == null) {
      return;
    }
    for(ColumnInfo eachColumn: tableSchema.getColumnInfos()) {
      if(eachColumn.getColumnType() == TableSchema.BLOB_TYPE) {
        Cell cell = row.getFirst(eachColumn.getColumnName());
        if(cell != null && cell.getBytes() != null) {
          removeBlob(eachColumn.getColumnName(), new BlobColumnCell(cell.getBytes()));
        }
      }
    }
  }
  
  private void removeBlob(String columnName, BlobColumnCell blobColumnCell) throws IOException {
    CTable blobTable = CTable.openTable(conf, BlobMetaManager.getBlobTableName(tableName, columnName));
    Row.Key rowKey = new Row.Key(blobColumnCell.getFilePath());

    /*
    Merger와 동기화 문제 해결 후 적용
    Row blobRow = blobTable.get(rowKey);
    List<Cell> cells = blobRow.getCellList(BLOB_COLUMN_FILE);
    if(cells == null || cells.size() == 0) {
      return;
    } 
    if(cells.size() == 1) {
      
    } else {
      Cell.Key cellKey = new Cell.Key(NBlobOutputStream.OFFSET_DF.format(blobColumnCell.getOffset()));
      Row row = new Row(rowKey);
      row.addCell(BLOB_COLUMN_DELETE, new Cell(cellKey, "Y".getBytes()));
      blobTable.put(row);
    }
    */
    Cell.Key cellKey = new Cell.Key(CBlobOutputStream.OFFSET_DF.format(blobColumnCell.getOffset()));
    Row row = new Row(rowKey);
    row.addCell(BLOB_COLUMN_DELETE, new Cell(cellKey, "Y".getBytes()));
    blobTable.put(row);
  }

  public long getTxTimeout() {
    return txTimeout;
  }
  
  public static void main(String[] args) throws Exception {
    if(args.length < 3) {
      System.out.println("Usage: java CTable <table name> <column name> <rk> [ck]");
      System.exit(0);
    }
    CTable ctable = CTable.openTable(new CloudataConf(), args[0]);
    
    String columnName = args[1];
    
    RowFilter rowFilter = new RowFilter();
    CellFilter cellFilter = null;
    if(args.length < 4) {
      cellFilter = new CellFilter(columnName);
    } else {
      cellFilter = new CellFilter(columnName, new Cell.Key(args[3]));
    }
    cellFilter.setTimestamp(Long.MIN_VALUE, Long.MAX_VALUE);
    rowFilter.setRowKey(new Row.Key(args[2].getBytes("utf-8")));
    rowFilter.addCellFilter(cellFilter);
    
    long startTime = System.currentTimeMillis();
    Row row = ctable.get(rowFilter);
    
    if(row == null || row.getFirstColumnCellList() == null) {
      System.out.println("no data");
      return;
    }
    
    row.print(columnName);
  }

  
}
