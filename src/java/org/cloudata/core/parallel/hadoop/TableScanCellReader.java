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
import org.cloudata.core.client.ScanCell;
import org.cloudata.core.client.scanner.ScannerFactory;
import org.cloudata.core.client.scanner.TableScanner;
import org.cloudata.core.common.conf.CloudataConf;

/**
 * @author jindolk
 *
 */
public class TableScanCellReader implements RecordReader<Row.Key, Writable> {
  static final Logger LOG = Logger.getLogger(TableScanCellReader.class.getName());
  private TableScanner scanner;
  private int readRowCount;
  private int TOTAL_ROW = 1000000;
  private boolean end = false;
  private ScanCell scanCell;
  private long totalScanTime;
  private JobConf jobConf;
  
  private boolean skipError = false;
  
  private Reporter reporter;
  
  public TableScanCellReader(JobConf jobConf, CloudataConf conf, TableSplit tableSplit, Reporter reporter) throws IOException {
    this.jobConf = jobConf;
    this.reporter = reporter;
    this.skipError = jobConf.getBoolean(AbstractTabletInputFormat.SKIP_ERROR, false);
    
    RowFilter rowFilter = tableSplit.getRowFilter();
    
    InputTableInfo inputTableInfo = tableSplit.getInputTableInfo();
    Row.Key startRowKey = rowFilter.getStartRowKey();
    
    reporter.setStatus(inputTableInfo.getTableName() + ":" + rowFilter.getStartRowKey() + " ~ " + rowFilter.getEndRowKey());
    
    CTable ctable = CTable.openTable(conf, inputTableInfo.getTableName());
    long startTime = System.currentTimeMillis();
    try {
      scanner = ScannerFactory.openScanner(ctable, rowFilter,
          TableScanner.SCANNER_OPEN_TIMEOUT);
    } catch (IOException e) {
      if(this.scanner != null) {
        this.scanner.close();
      }
      end = true;
      LOG.error("Error while scanner open:startRowKey=" + startRowKey + "," + e.getMessage());
      if(skipError) {
        reporter.setStatus("Error while scanner open:startRowKey=" + startRowKey + ", endRowKey=" + e.getMessage());
        reporter.incrCounter("cloudata", "error", 1);
        return;
      } else {
        throw e;
      }
    }
    
    try {
      //첫번째 Tablet이 아닌 경우에는 첫번째 row는 무시한다.
      scanCell = scanner.next();
      if(scanCell == null) {
        end = true;
        scanner.close();
        scanner = null;
        return;
      }
      if(!startRowKey.equals(Row.Key.MIN_KEY)) {
        Row.Key firstRow = scanCell.getRowKey();
        
        while(firstRow.equals(scanCell.getRowKey())) {
          scanCell = scanner.next();
          if(scanCell == null) {
            end = true;
            scanner.close();
            scanner = null;
            break;
          }
        }
      } 
      
      totalScanTime += (System.currentTimeMillis() - startTime);
    } catch (IOException e) {
      if(scanner == null) {
        scanner.close();
        scanner = null;
      }
      LOG.error("Error while skip first row:startRowKey=" + startRowKey + "," + e.getMessage());
      if(skipError) {
        reporter.setStatus("Error while skip first row:startRowKey=" + startRowKey + ", endRowKey=" + e.getMessage());
        reporter.incrCounter("cloudata", "error", 1);
        return;
      } else {
        throw e;
      }      
    }
  }

  public void close() throws IOException {
    LOG.debug("Total Scan Time:" + totalScanTime + " ms");
    if(scanner != null) {
      scanner.close();
      scanner = null;
    }
  }

  @Override
  public Row.Key createKey() {
    return new Row.Key();
  }

  @Override
  public ScanCell createValue() {
    ScanCell scanCell = new ScanCell();
    return scanCell;
  }

  @Override
  public long getPos() {
    return readRowCount;
  }

  @Override
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

  @Override
  public boolean next(Row.Key key, Writable value) throws IOException {
    boolean hasMore = (scanCell != null);
    if(!end && hasMore) {
      Row.Key rowKey = scanCell.getRowKey();
      key.set(rowKey.getBytes());
      
      readRowCount++;
      
      ScanCell result = (ScanCell)value;
      result.setRowKey(rowKey);
      result.setColumnName(scanCell.getColumnName());
      result.setCellKey(scanCell.getCellKey());
      result.setCellValue(scanCell.getCellValue());
      
      try {
        long startTime = System.currentTimeMillis();
        scanCell = scanner.next();
        totalScanTime += (System.currentTimeMillis() - startTime);
      } catch (IOException e) {
        end = true;
        LOG.error("Error while scanner.next():" + e.getMessage());
        reporter.setStatus("Error while scanner.next():" + e.getMessage());
        if(skipError) {
          reporter.incrCounter("cloudata", "error", 1);
          return false;
        } else {
          throw e;
        }  
      }
    }
    end = !hasMore;
    
    if(!hasMore) {
      LOG.info("end of data");
      if(scanner != null) {
        scanner.close();
        scanner = null;
      }
    }
    return hasMore;
  }
}
