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
import org.cloudata.core.client.scanner.ScannerFactory;
import org.cloudata.core.client.scanner.TableScanner;
import org.cloudata.core.common.conf.CloudataConf;

/**
 * @author jindolk
 *
 */
public class TableRowRecordReader implements RecordReader<Row.Key, Writable> {
  static final Logger LOG = Logger.getLogger(TableRowRecordReader.class.getName());
  private TableScanner scanner;
  private int readRowCount;
  private int TOTAL_ROW = 1000000;
  private boolean end = false;
  
  private Row.Key startRowKey;
  private Row.Key endRowKey;
  
  private JobConf jobConf;
  
  private boolean skipError = false;
  
  private Reporter reporter;
  
  public TableRowRecordReader(JobConf jobConf, CloudataConf conf, TableSplit tableSplit, Reporter reporter) throws IOException {
    this.jobConf = jobConf;
    this.reporter = reporter;
    this.skipError = jobConf.getBoolean(AbstractTabletInputFormat.SKIP_ERROR, false);
    
    RowFilter rowFilter = tableSplit.getRowFilter();
    InputTableInfo inputTableInfo = tableSplit.getInputTableInfo();
    
    this.startRowKey = rowFilter.getStartRowKey();
    this.endRowKey = rowFilter.getEndRowKey();
    
    LOG.info("TableSplit: " + inputTableInfo.getTableName() + ":" + startRowKey + " ~ " + endRowKey);
    if(reporter != null) {
      reporter.setStatus(inputTableInfo.getTableName() + ":" + startRowKey + " ~ " + endRowKey);
    }
    
    CTable ctable = CTable.openTable(conf, inputTableInfo.getTableName());
    try {
      this.scanner = ScannerFactory.openScanner(ctable, rowFilter, TableScanner.SCANNER_OPEN_TIMEOUT);
    } catch (IOException e) {
      if(this.scanner != null) {
        this.scanner.close();
      }
      end = true;
      LOG.error("Error while scanner open:startRowKey=" + startRowKey + ", endRowKey=" + endRowKey, e);
      if(skipError) {
        reporter.setStatus("Error while scanner open:startRowKey=" + startRowKey + ", endRowKey=" + endRowKey + "," + e.getMessage());
        reporter.incrCounter("cloudata", "error", 1);
        return;
      } else {
        throw e;
      }
    } 
    
    //If Tablet isn't first tablet, skip first row.
    if(!startRowKey.equals(tableSplit.getJobStartRowKey())) {
      try {
        Row row = scanner.nextRow();
        if(row == null) {
          end = true;
          return;
        }
      } catch (IOException e) {
        end = true;
        LOG.error("Error while scanner.nextRow():startRowKey=" + startRowKey + ", endRowKey=" + endRowKey, e);
        if(skipError) {
          reporter.setStatus("Error while scanner.nextRow():startRowKey=" + startRowKey + ", endRowKey=" + endRowKey + "," + e.getMessage());
          reporter.incrCounter("cloudata", "error", 1);
          return;
        } else {
          throw e;
        }
      }
    }
  }
  
  @Override
  public void close() throws IOException {
    end = true;
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
  public Writable createValue() {
    Row row = new Row();
    return row;
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
    if(end) {
      return false;
    }
    Row row = null;
    try {
      row = scanner.nextRow();
    } catch(IOException e) {
      end = true;
      LOG.error("nextRow error:range=" + startRowKey + "," + endRowKey + "," + e.getMessage(), e);
      if(skipError) {
        reporter.setStatus("Error while nextRow:startRowKey=" + startRowKey + ", endRowKey=" + endRowKey + "," + e.getMessage());
        reporter.incrCounter("cloudata", "error", 1);
        return false;
      } else {
        throw e;
      }
    }
    
    boolean hasMore = (row != null);
    
    if(!end && hasMore) {
      readRowCount++;
      Row.Key rowKey = row.getKey();
      
      key.set(rowKey.getBytes());

      Row result = (Row)value;

      result.setKey(rowKey);
      result.setAllCells(row.getAllCells());
    }
    end = !hasMore;
    if(!hasMore) {
      if(scanner != null) {
        scanner.close();
        scanner = null;
      }
    }
    return hasMore;
  }

}
