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
package org.cloudata.util.matrix;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
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
import org.cloudata.core.parallel.hadoop.InputTableInfo;
import org.cloudata.core.parallel.hadoop.TableSplit;
import org.cloudata.core.tablet.TabletInfo;


public class MatrixInputFormat  implements InputFormat, JobConfigurable {
  static final Logger LOG = Logger.getLogger(MatrixInputFormat.class.getName());
  public static final String MATRIX_INPUT_TABLE = "cloudata.matrix.input.table";
  public static final String MATRIX_INPUT_COLUMN = "cloudata.matrix.input.column";
  public static final String MATRIX_TARGET_TABLE = "cloudata.matrix.target.table";
  public static final String MATRIX_TARGET_COLUMN = "cloudata.matrix.target.column";
  public static final String MATRIX_TARGET_SPARSE = "cloudata.matrix.target.sparse";
  public static final String MATRIX_RESULT_TABLE = "cloudata.matrix.result.table";
  public static final String MATRIX_RESULT_COLUMN = "cloudata.matrix.result.column";
  public static final String MATRIX_RESULT_SPARSE = "cloudata.matrix.result.sparse";
  
  private CTable ctable;
  private CloudataConf conf;
  private String columnName;

  public MatrixInputFormat() throws IOException {
    conf = new CloudataConf();
  }
  
  static class MatrixRecordReader implements RecordReader<WritableComparable, Writable> {
    private TableScanner scanner;
    private int readRowCount;
    private int TOTAL_ROW = 1000000;
    private boolean end = false;
    private ScanCell scanCell;
    private long totalScanTime;
    
    public MatrixRecordReader(
        CloudataConf conf, 
        String tableName,
        String columnName,
        Row.Key startRowKey,
        Row.Key endRowKey) throws IOException {
      CTable ctable = CTable.openTable(conf, tableName);
      long startTime = System.currentTimeMillis();
      scanner = ScannerFactory.openScanner(ctable, startRowKey, endRowKey, columnName);
      
      //첫번째 Tablet이 아닌 경우에는 첫번째 row는 무시한다.
      scanCell = scanner.next();
      if(scanCell == null) {
        end = true;
        return;
      }
      if(!startRowKey.equals(Row.Key.MIN_KEY)) {
        Row.Key firstRow = scanCell.getRowKey();
        
        while(firstRow.equals(scanCell.getRowKey())) {
          scanCell = scanner.next();
          if(scanCell == null) {
            end = true;
            break;
          }
        }
      } 
      totalScanTime += (System.currentTimeMillis() - startTime);
    }

    public void close() throws IOException {
      scanner.close();
    }

    public WritableComparable createKey() {
      return new Text();
    }

    public Writable createValue() {
      return new MatrixItem();
    }

    public long getPos() {
      return readRowCount;
    }

    public float getProgress() {
//    TODO tablet의 전체 row건수를 알고 있어야만 계산 가능
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

    public boolean next(WritableComparable key, Writable value) throws IOException {
      boolean hasMore = (scanCell != null);
      if(!end && hasMore) {
        Row.Key rowKey = scanCell.getRowKey();
        
        readRowCount++;
        
        Text tKey = (Text)key;
        tKey.set(rowKey.getBytes());

        MatrixItem paramMatrixItem = (MatrixItem)value;
        paramMatrixItem.setRow(scanCell.getRowKey().toString());
        paramMatrixItem.setColumn(scanCell.getCellKey().toString());
        paramMatrixItem.setValue(Double.parseDouble(new String(scanCell.getBytes())));
        
        try {
          //long startTime = System.currentTimeMillis();
          scanCell = scanner.next();
          //totalScanTime += (System.currentTimeMillis() - startTime);
        } catch (IOException e) {
          throw e; 
        }
      }
      end = !hasMore;
      
      return hasMore;
    }
  }
  
  public RecordReader getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
    TableSplit tSplit = (TableSplit)split;
    return new MatrixRecordReader(conf, 
        tSplit.getInputTableInfo().getTableName(),
        columnName,
        tSplit.getRowFilter().getStartRowKey(),
        tSplit.getRowFilter().getEndRowKey());
  }

  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    //LOG.debug("start getSplits");
    TabletInfo[] tabletInfos = ctable.listTabletInfos();
    
    if(tabletInfos == null || tabletInfos.length == 0) {
      throw new IOException("Table has at least one tablet");
    }
    
    InputSplit[] splits = new InputSplit[tabletInfos.length];
    Row.Key startRowKey = Row.Key.MIN_KEY;
    for(int i = 0; i < tabletInfos.length; i++) {
      RowFilter rowFilter = new RowFilter();
      rowFilter.setStartRowKey(startRowKey);
      rowFilter.setEndRowKey(tabletInfos[i].getEndRowKey());
      InputTableInfo inputTableInfo = new InputTableInfo();
      inputTableInfo.setTable(ctable.getTableName(), rowFilter);
      splits[i] = new TableSplit(inputTableInfo, 
          tabletInfos[i].getAssignedHostName(),
          rowFilter
          );
      startRowKey = tabletInfos[i].getEndRowKey();
    }
    return splits;
  }

  public void configure(JobConf job) {
    try {
      this.ctable = CTable.openTable(conf, job.get(MATRIX_INPUT_TABLE));
    } catch (Exception e) {
      e.printStackTrace();
    }
    this.columnName = job.get(MATRIX_INPUT_COLUMN);
  }

  public void validateInput(JobConf job) throws IOException {
    String tableName = job.get(MATRIX_INPUT_TABLE);
    if(tableName == null || tableName.trim().length() == 0) {
      throw new IOException("expecting " + MATRIX_INPUT_TABLE + " for input table");
    }
    
    String column = job.get(MATRIX_INPUT_COLUMN);
    if(column == null || column.length() == 0) {
      throw new IOException("expecting one column");
    }
  }
}
