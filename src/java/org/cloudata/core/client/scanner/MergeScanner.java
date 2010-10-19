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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import org.cloudata.core.client.CellFilter;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.RowFilter;
import org.cloudata.core.client.Row.Key;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.io.CWritable;


/**
 * MergeScanner scans data from several Tables. It runs like JOIN operation in RDBMS.<BR>
 * select T1.*, T2.* from T1, T2 where T1.rowkey = T2.rowkey  
 *
 */
public class MergeScanner {
  private TableScanner[] scanners;
  private boolean end = false;
  private SortedSet<RowItem> buffers = new TreeSet<RowItem>();
  
  private List<String[]> columnNames = new ArrayList<String[]>();
  private int columnLength;
  
  //scannerIndex, result column position
  private Map<Integer, Integer> columnPositions = new HashMap<Integer, Integer>();
  
  private MergeEvaluator mergeEvaluator;
  
  public MergeScanner(TableScanner[] scannerArray) throws IOException {
    this(scannerArray, new DefaultMergeEvaluator());
  }
  
  public MergeScanner(TableScanner[] scannerArray, MergeEvaluator mergeEvaluator) throws IOException {
    this.scanners = scannerArray;
    this.mergeEvaluator = mergeEvaluator;
    for(int i = 0; i < scanners.length; i++) {
      Row row = scanners[i].nextRow();
      if(isEmpty(row)) {
        scanners[i].close();
        scanners[i] = null;
        continue;
      }
      RowItem rowItem = new RowItem(row, i, mergeEvaluator);
      buffers.add(rowItem);
      String[] eachScannerColumns = scannerArray[i].getColumnNames();
      this.columnNames.add(eachScannerColumns);
      columnPositions.put(i, columnLength);
      columnLength += eachScannerColumns.length;
    }
  }
  
  private boolean isEmpty(Row row) {
    return row == null || row.getColumnCount() == 0;
  }
  
  /**
   * 
   * @throws IOException
   */
  public void close() throws IOException {
    for(TableScanner eachScanner: scanners) {
      if(eachScanner != null) {
        eachScanner.close();
      }
    }
  }

  /**
   * Fetch next row.<BR>
   * If exists in a table(T1) and no data in a other table(T2), return T1's data and null.<BR>
   * It is similar to OUTER JOIN
   * @return
   * @throws IOException
   */
  public RowArray nextRow() throws IOException {
    //TODO 현재는 양쪽에 하나라도 있으면 모두 나간다.
    //향후 JOIN 옵션(left, right outer join)에 따라 구성
    if(end) {
      return null;
    }
    
    if(buffers.size() == 0) {
      end = true;
      return null;
    }
    
    RowItem winnerItem = buffers.first();
    int winnerIndex = winnerItem.index;
    buffers.remove(winnerItem);

    //scan winner scanner & add buffer
    Row nextRow = scan(winnerIndex);
    if(nextRow != null) {
      RowItem rowItem = new RowItem(nextRow, winnerIndex, mergeEvaluator);
      buffers.add(rowItem);
    } 
    
    RowArray result = new RowArray();
    result.rowKey = winnerItem.row.getKey();
    
    Row[] rows = new Row[scanners.length];
    
    rows[winnerIndex] = winnerItem.row;

    //현재 buf에 있는 row 중에 동일한 rowkey를 가지고 있으면 결과에 추가한다.
    //TODO outer, inner join 옵션에 따라 모두 있는 경우에만 결과 반환(현재는 outer join)
    List<RowItem> tempItems = new ArrayList<RowItem>();
    tempItems.addAll(buffers);
    for(RowItem eachItem: tempItems) {
      if(mergeEvaluator.eval(winnerItem.row, winnerItem.index, eachItem.row, eachItem.index)) {
      //if(rowKey.equals(eachItem.getRowKey())) {
        int itemIndex = eachItem.index;
        rows[itemIndex] = eachItem.row;
        
        buffers.remove(eachItem);

        nextRow = scan(itemIndex);
        if(nextRow != null) {
          RowItem rowItem = new RowItem(nextRow, itemIndex, mergeEvaluator);
          buffers.add(rowItem);
        }
      }
    }
    
    result.rows = rows;
    
    return result;
  }
  
  
  private Row scan(int scannerIndex) throws IOException {
    if(scanners[scannerIndex] == null) {
      return null;
    }
    Row row = scanners[scannerIndex].nextRow();
    if(isEmpty(row)) {
      scanners[scannerIndex].close();
      scanners[scannerIndex] = null;
      return null;
    }    
    return row;
  }
  
  class RowItem implements Comparable<RowItem> {
    int index;
    Row row;
    MergeEvaluator mergeEvaluator;
    public RowItem(Row row, int index, MergeEvaluator mergeEvaluator) {
      this.index = index;
      this.row = row;
      this.mergeEvaluator = mergeEvaluator;
    }

    public Row.Key getRowKey() {
      return row.getKey();
    }
    
    public int compareTo(RowItem rowItem) {
      //동일한 rowKey의 경우 index가 빠른 scanner의 값을 반환
      Row.Key rowKey = mergeEvaluator.parseTargetRowKey(getRowKey(), index);
      Row.Key otherRowKey = mergeEvaluator.parseTargetRowKey(rowItem.getRowKey(), rowItem.index);
      
      if(rowKey.compareTo(otherRowKey) == 0) {
        if(index > rowItem.index) {
          return 1;
        } else if( index == rowItem.index) {
          return 0;
        } else {
          return -1;
        }
      } else {
        return rowKey.compareTo(otherRowKey);
      }
    }
    
    public boolean equals(Object obj) {
      if(!(obj instanceof RowItem)) {
        return false;
      }
      
      return compareTo((RowItem)obj) == 0;
    }
  }
  
  static class NullRow extends Row {
    @Override
    public void readFields(DataInput in) throws IOException {
      in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(0);
    }
  }
  
  /**
   * Value Object which stores row array. 
   *
   */
  public static class RowArray implements CWritable, Comparable<RowArray>, Writable {
    Row.Key rowKey;
    
    Row[] rows;
    
    public RowArray() {
      
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
      rowKey = new Row.Key();
      rowKey.readFields(in);
      
      int length = in.readInt();
      
      rows = new Row[length];
      
      for(int i = 0; i < length; i++) {
        int rowLength = in.readInt();
        if(rowLength > 0) {
          rows[i] = new Row();
          rows[i].readFields(in);
        } 
      }
    }

    @Override
    public void write(DataOutput out) throws IOException {
      rowKey.write(out);
      out.write(rows.length);
      
      for(int i = 0; i < rows.length; i++) {
        if(rows[i] == null) {
          out.writeInt(0);
        } else {
          out.writeInt(1);
          rows[i].write(out);
        }
      }
    }

    @Override
    public int compareTo(RowArray o) {
      return rowKey.compareTo(o.rowKey);
    }
    
    public boolean equals(Object obj) {
      if( !(obj instanceof RowArray) ) {
        return false;
      }
      
      return compareTo((RowArray)obj) == 0;
    }
    
    public Row.Key getRowKey() {
      return rowKey;
    }
    
    public Row[] getRows() {
      return rows;
    }

    public void setRowKey(Row.Key rowKey) {
      this.rowKey = rowKey;
    }

    public void setRows(Row[] rows) {
      this.rows = rows;
    }
  }
  
  static class DefaultMergeEvaluator implements MergeEvaluator {

    @Override
    public boolean eval(Row row, int index, Row row2, int index2) {
      return row.getKey().equals(row2.getKey());
    }

    @Override
    public Key parseTargetRowKey(Key srcRowKey, int index) {
      return srcRowKey;
    }
  }
  
}
