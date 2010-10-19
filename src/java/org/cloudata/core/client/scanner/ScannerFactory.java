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
package org.cloudata.core.client.scanner;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.client.CellFilter;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.CTableManager;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.RowFilter;
import org.cloudata.core.client.TabletLocationCache;
import org.cloudata.core.client.Row.Key;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.tablet.TabletInfo;


/**
 * ScannerFactory creates TableScanner object.<BR>
 * There is various scanner in Cloudata<BR>
 * Basic scanner is SingleTabletScanner which scans data in a single tablet, single column.<BR>
 * SingleTabletMultiColumnScanner scans n a single tablet, multiple columns. SingleTabletMultiColumnScanner uses several SingleTabletScanner<BR>
 *
 */
public class ScannerFactory {
  private static final Log LOG = LogFactory.getLog(ScannerFactory.class.getName());
  
  /**
   * Open TableScanner which scans from all tablets, single column.
   * @param ctable
   * @param columnName
   * @return
   * @throws IOException
   */
  public static TableScanner openScanner(CTable ctable, String columnName) throws IOException {
    if(columnName == null) {
      throw new IOException("columnName is null");
    }
    RowFilter rowFilter = new RowFilter();
    rowFilter.addCellFilter(new CellFilter(columnName));
    return openScanner(ctable, rowFilter, TableScanner.SCANNER_OPEN_TIMEOUT);
  }

  /**
   * Open TableScanner which scans from all tablets, multiple column.
   * @param ctable
   * @param columns
   * @return
   * @throws IOException
   */
  public static TableScanner openScanner(CTable ctable, String[] columns) throws IOException {
    if(columns == null) {
      throw new IOException("columns is null");
    }
    RowFilter rowFilter = new RowFilter();
    for(String eachColumn: columns) {
      if(eachColumn == null) {
        throw new IOException("column is null");
      }
      rowFilter.addCellFilter(new CellFilter(eachColumn));
    }
    return openScanner(ctable, rowFilter, TableScanner.SCANNER_OPEN_TIMEOUT);
  }
  
  /**
   * Open TableScanner which scans from startRowKey to endRowKey, single column.
   * @param ctable
   * @param startRowKey
   * @param endRowKey
   * @param columnName
   * @return
   * @throws IOException
   */
  public static TableScanner openScanner(CTable ctable, Row.Key startRowKey,
      Row.Key endRowKey, String columnName) throws IOException {
    if(columnName == null) {
      throw new IOException("columnName is null");
    }
    return openScanner(ctable, startRowKey, endRowKey, columnName, TableScanner.SCANNER_OPEN_TIMEOUT);
  }

  /**
   * Open TableScanner which scans from startRowKey to endRowKey, single column.
   * @param ctable
   * @param startRowKey
   * @param endRowKey
   * @param columnName
   * @param timeout
   * @return
   * @throws IOException
   */
  public static TableScanner openScanner(CTable ctable, Row.Key startRowKey,
      Row.Key endRowKey, String columnName, int timeout) throws IOException {
    if(columnName == null) {
      throw new IOException("columnName is null");
    }
    RowFilter rowFilter = new RowFilter(startRowKey, endRowKey);
    rowFilter.addCellFilter(new CellFilter(columnName));
    return openScanner(ctable, rowFilter, timeout); 
  }

  /**
   * Open TableScanner which scans from startRowKey to endRowKey, multiple column.
   * @param ctable
   * @param startRowKey
   * @param endRowKey
   * @param columns
   * @return
   * @throws IOException
   */
  public static TableScanner openScanner(CTable ctable, Key startRowKey,
      Key endRowKey, String[] columns) throws IOException {
    RowFilter rowFilter = new RowFilter(startRowKey, endRowKey);
    for(String eachColumn: columns) {
      rowFilter.addCellFilter(new CellFilter(eachColumn));
    }
    return openScanner(ctable, rowFilter, TableScanner.SCANNER_OPEN_TIMEOUT);
  }

  /**
   * Open TableScanner which scans with various condition. You can set scan condition with RowFilter and CellFilter.
   * @param ctable
   * @param rowFilter
   * @return
   * @throws IOException
   */
  public static TableScanner openScanner(CTable ctable, RowFilter rowFilter) throws IOException {
    return openScanner(ctable, rowFilter, TableScanner.SCANNER_OPEN_TIMEOUT);     
  }

  /**
   * Open TableScanner which scans with various condition. You can set scan condition with RowFilter and CellFilter.
   * @param ctable
   * @param rowFilter
   * @param timeout
   * @return
   * @throws IOException
   */
  public static TableScanner openScanner(CTable ctable, RowFilter rowFilter, int timeout) throws IOException {
    checkColumn(rowFilter);
    
    if(rowFilter.getColumns().length == 1) {
      return new MultiTabletScanner(ctable, rowFilter, timeout);
    } else {
      return new MultiTabletMultiColumnScanner(ctable, rowFilter, timeout);
    }
  }

  /**
   * Open TableScanner which scans a single tablet, multiple columns.
   * @param conf
   * @param tabletInfo
   * @param columnNames
   * @return
   * @throws IOException
   */
  public static TableScanner openScanner(CloudataConf conf, TabletInfo tabletInfo, String[] columnNames) throws IOException {
    RowFilter rowFilter = new RowFilter();
    for(String eachColumn: columnNames) {
      rowFilter.addCellFilter(new CellFilter(eachColumn));
    }
    
    return openScanner(conf, tabletInfo.getEndRowKey(), tabletInfo, rowFilter);
  }

  /**
   * Open TableScanner which scans a single tablet, single column.
   * @param conf
   * @param tabletInfo
   * @param columnName
   * @return
   * @throws IOException
   */
  public static TableScanner openScanner(CloudataConf conf, TabletInfo tabletInfo, String columnName) throws IOException {
    RowFilter rowFilter = new RowFilter();
    rowFilter.addCellFilter(new CellFilter(columnName));
    
    return openScanner(conf, tabletInfo.getEndRowKey(), tabletInfo, rowFilter);
  }

  /**
   * Open TableScanner which scans a single tablet used RowFilter
   * @param conf
   * @param tabletInfo
   * @param rowFilter
   * @return
   * @throws IOException
   */
  public static TableScanner openScanner(CloudataConf conf,
      Row.Key targetRowKey,
      TabletInfo tabletInfo, 
      RowFilter rowFilter) throws IOException {
    if(rowFilter.getColumns().length == 1) {
      return openSingleTabletScanner(conf, targetRowKey, tabletInfo, rowFilter);
    } else {
      return new SingleTabletMultiColumnScanner(conf, targetRowKey, tabletInfo, rowFilter);
    }
  }
  
  private static TableScanner openSingleTabletScanner(CloudataConf conf,
      Row.Key targetRowKey,
      TabletInfo tabletInfo, 
      RowFilter rowFilter) throws IOException {
    
    if(tabletInfo == null) {
      return null;
    }
    
    IOException exception = null;
    
    long txTimeout = conf.getInt("client.tx.timeout", 60) * 1000;
    long startTime = System.currentTimeMillis();
    
    String tableName = tabletInfo.getTableName();
    while(true) {
      try {
        SingleTabletScanner tableScanner = new SingleTabletScanner(conf, tabletInfo, rowFilter);
        return tableScanner;
      } catch (IOException e) {
        //LOG.warn("Scanner open error cause:" + e.getMessage() + " but retry, tablet:" + tabletInfo + ", error:" + e.getMessage());
        exception = e;
      } catch (Exception e) {
        LOG.error("Scanner open error cause:" + e.getMessage() + ", tablet:" + tabletInfo, e);
        exception = new IOException(e.getMessage());
        exception.initCause(e);
        throw exception;
      }
      
      if(tabletInfo != null) {
        TabletLocationCache.getInstance(conf).clearTabletCache(tabletInfo.getTableName(), targetRowKey, tabletInfo);
      }
      try {
        tabletInfo = CTable.lookupTargetTablet(conf, tableName, targetRowKey);
      } catch (Exception err) {
        LOG.error("Scanner open error while lookupTargetTablet cause:" + err.getMessage() + " but retry:" + 
            tabletInfo.getTableName() + ",rowKey=" + targetRowKey);
      }
      
      if( (System.currentTimeMillis() - startTime) > txTimeout) {
        break;
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
    }
    
    if(exception == null) {
      throw new IOException("fail open Scanner " + txTimeout + " ms tabletInfo=" + tabletInfo);
    }  else {
      LOG.warn("Can't open scanner:" + tabletInfo + "," + exception.getMessage());
      throw CTableManager.makeIOException(exception);
    } 
  }
  
  private static void checkColumn(RowFilter rowFilter) throws IOException {
    if(rowFilter.getColumns() == null || rowFilter.getColumns().length == 0) {
      throw new IOException("TableScanner must have only one column");
    }    
  }
}
