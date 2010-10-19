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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.tablet.ColumnValue;
import org.cloudata.core.tablet.TabletInfo;


/**
 * DirectUploader is used to upload large data set.<BR>
 * DirectUploader uploads large data to FileSystem and adds file list to TabletServer.<BR>
 * <pre>
 * DirectUploader uploader = null;
 * try {
 *   uploader = ctable.openDirectUploader(new String[]{"Column1", "Column2"}, false);
 *   for(...) {
 *     Row row = new Row(...);
 *     row.addCell(...);
 *     uploader.put(row);
 *   }
 *   uploader.close();
 * } catch (Exception e) {
 *   if(uploader != null) {
 *     uploader.rollback();
 *   }
 * } 
 * </pre>
 * 
 * Table must be partitioned before using DirectUploader. And datas for a single DirectUploader must have similar rowkey range.<BR>
 */
public abstract class DirectUploader {
  protected static final Log LOG = LogFactory.getLog(DirectUploader.class.getName());
  
  protected CTable ctable;
  
  protected String[] columnNames;
  
  protected TabletInfo[] tabletInfos;
  
  protected TabletInfo currentTabletInfo;
  
  protected Row.Key previousTabletEndRowKey;
  
  protected DirectUploader(CTable ctable, String[] columnNames) throws IOException {
    this.ctable = ctable;
    this.columnNames = new String[columnNames.length];
    
    for(int i = 0; i < columnNames.length; i++) {
      this.columnNames[i] = columnNames[i].trim();
    }
    previousTabletEndRowKey = Row.Key.MIN_KEY;
  }
  
  /**
   * Put data to DirectUploader
   * @param rowKey
   * @param columnValues 
   * @throws IOException
   */
  public abstract void put(Row row) throws IOException;
  
  /**
   * Rollback uploaded datas. Clear and ignore all datas.<BR>
   * If failed while putting or uploading after DirectUploader open, You must call rollback method.
   * @throws IOException
   */
  public abstract void rollback() throws IOException;
  
  /**
   * Send data to DFS, and add data file to TabletServer.<BR>
   * After putting all datas, you must call close() method
   * @throws IOException
   */
  public abstract void close() throws IOException;
  
  protected ColumnValue[][] rowToColumnValue(Row row) {
    ColumnValue[][] columnValues = new ColumnValue[columnNames.length][]; 
    
    int count = 0;
    
    for(int i = 0; i < columnNames.length; i++) {
      List<Cell> cells = row.getCellList(columnNames[i]);
      List<ColumnValue> columnValueList = new ArrayList<ColumnValue>();
      if(cells == null || cells.size() == 0) {
        columnValues[i] = new ColumnValue[]{};
        continue;
      }
      
      for(Cell eachCell: cells) {
        for(Cell.Value eachValue: eachCell.getValues()) {
          ColumnValue columnValue = new ColumnValue();
          columnValue.setRowKey(row.getKey());
          columnValue.setCellKey(eachCell.getKey());
          eachValue.copyToColumnValue(columnValue);
          columnValueList.add(columnValue);
          count++;
        }
      }
      
      columnValues[i] = columnValueList.toArray(new ColumnValue[]{});
    }
    
    if(count == 0) {
      return null;
    } else {
      return columnValues;
    }
  }
}
