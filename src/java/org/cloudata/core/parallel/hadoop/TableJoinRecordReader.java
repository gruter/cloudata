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
package org.cloudata.core.parallel.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.RowFilter;
import org.cloudata.core.client.scanner.MergeEvaluator;
import org.cloudata.core.client.scanner.MergeScanner;
import org.cloudata.core.client.scanner.ScannerFactory;
import org.cloudata.core.client.scanner.TableScanner;
import org.cloudata.core.client.scanner.MergeScanner.RowArray;
import org.cloudata.core.common.conf.CloudataConf;

/**
 * @author jindolk
 *
 */
public class TableJoinRecordReader implements RecordReader<Row.Key, Writable> {
  static final Logger LOG = Logger.getLogger(TableJoinRecordReader.class.getName());
  
  private CloudataConf conf;
  private MergeScanner scanner;
  private int readRowCount;
  private int TOTAL_ROW = 1000000;
  private boolean end = false;
  private Row.Key startRowKey;
  private Row.Key endRowKey;
  
  public TableJoinRecordReader(JobConf jobConf, CloudataConf conf, TableSplit tableSplit, Reporter reporter) throws IOException {
    this.conf = conf;

    String mergeEvaluatorClass = tableSplit.getInputTableInfo().getMergeEvaluatorClass();
    MergeEvaluator mergeEvaluator = null;
    if(mergeEvaluatorClass != null && mergeEvaluatorClass.length() > 0) {
      try {
        mergeEvaluator = (MergeEvaluator)Class.forName(mergeEvaluatorClass).newInstance();
      } catch (Exception e) {
        LOG.error("mergeEvaluator:" + mergeEvaluatorClass + "," + e.getMessage());
        IOException err = new IOException(e.getMessage() + ":" + mergeEvaluatorClass);
        err.initCause(e);
        throw err;
      }
    }
      
    RowFilter splitRowFilter = tableSplit.getRowFilter();
    InputTableInfo inputTableInfo = tableSplit.getInputTableInfo();
    
    this.startRowKey = splitRowFilter.getStartRowKey();
    this.endRowKey = splitRowFilter.getEndRowKey();
    
    RowFilter rowFilter = inputTableInfo.getRowFilter();
    rowFilter.setStartRowKey(startRowKey);
    rowFilter.setEndRowKey(endRowKey);
    
    CTable ctable = CTable.openTable(conf, inputTableInfo.getTableName());

    TableScanner pivotScanner = null;
    TableScanner targetScanner = null;
    try {
      pivotScanner = ScannerFactory.openScanner(ctable, rowFilter, 
          TableScanner.SCANNER_OPEN_TIMEOUT);
      Row.Key firstRowKey = null;
      try {
        //첫번째 Tablet이 아닌 경우에는 첫번째 row는 무시한다.
        if(!startRowKey.equals(Row.Key.MIN_KEY)) {
          Row pivotRow = pivotScanner.nextRow();
          if(pivotRow == null) {
            end = true;
            return;
          }
          
          if(firstRowKey == null) {
            firstRowKey = pivotRow.getKey();
          }
          
          if(firstRowKey.equals(pivotRow.getKey())) {
            pivotRow = pivotScanner.nextRow();
            if(pivotRow == null) {
              end = true;
              return;
            }
          }
          pivotScanner.close();
          rowFilter.setStartRowKey(firstRowKey);
          pivotScanner = ScannerFactory.openScanner(ctable, 
              rowFilter, TableScanner.SCANNER_OPEN_TIMEOUT);
        } else {
          firstRowKey = startRowKey;
        }
      } catch (Exception e) {
        if(pivotScanner != null) {
          pivotScanner.close();
        }
        throw e;
      }
      
      RowFilter joinRowFilter = inputTableInfo.getJoinRowFilter();
      
      if(mergeEvaluator != null) {
        if(!firstRowKey.equals(Row.Key.MIN_KEY)) {
          joinRowFilter.setStartRowKey(mergeEvaluator.parseTargetRowKey(firstRowKey, 0));
        } else {
          joinRowFilter.setStartRowKey(Row.Key.MIN_KEY);
        }
        if(!rowFilter.getEndRowKey().equals(Row.Key.MAX_KEY)) {
          joinRowFilter.setEndRowKey(mergeEvaluator.parseTargetRowKey(rowFilter.getEndRowKey(), 0));
        } else {
          joinRowFilter.setEndRowKey(Row.Key.MAX_KEY);
        }
      } else {
        joinRowFilter.setStartRowKey(firstRowKey);
        joinRowFilter.setEndRowKey(rowFilter.getEndRowKey());
      }

      reporter.setStatus(inputTableInfo.getTableName() + ":" + startRowKey + " ~ " + endRowKey + ", " +
          inputTableInfo.getJoinTableName() + ":" + joinRowFilter.getStartRowKey() + " ~ " + joinRowFilter.getEndRowKey());
      
      //pivot table의 startRow, endRow에 대응하는 target table의 scanner 생성
      CTable targetTable = CTable.openTable(conf, inputTableInfo.getJoinTableName());
      targetScanner = ScannerFactory.openScanner(targetTable, 
          joinRowFilter,
          TableScanner.SCANNER_OPEN_TIMEOUT);
      
      this.scanner = new MergeScanner(new TableScanner[]{pivotScanner, targetScanner}, mergeEvaluator);
    } catch (Exception e) {
      if(targetScanner != null) {
        targetScanner.close();
      }
      IOException err = new IOException(e.getMessage());
      err.initCause(e);
      throw err;
    } 
  }

  public void close() throws IOException {
    if(scanner != null) {
      scanner.close();
    }
  }

  public Row.Key createKey() {
    return new Row.Key();
  }

  public RowArray createValue() {
    return new RowArray();
  }

  public long getPos() {
    return readRowCount;
  }

  public float getProgress() {
//  TODO tablet의 전체 row건수를 알고 있어야만 계산 가능 
    if(end) {
      return 1.0f;
    } else {
      float progress = (float)readRowCount/(float)TOTAL_ROW;
      if(progress >= 1.0) {
        return 0.98f;
      } else {
        return progress;
      }
    }
  }

  public boolean next(Row.Key key, Writable value) throws IOException {
    RowArray rows = scanner.nextRow();
    if(rows != null) {
      readRowCount++;
      Row.Key rowKey = rows.getRowKey();
      
      key.set(rowKey.getBytes());

      ((RowArray)value).setRowKey(rowKey);
      ((RowArray)value).setRows(rows.getRows());
      end = false;
      return true;
    } else {
      end = true;
      return false;
    }
  }
}
