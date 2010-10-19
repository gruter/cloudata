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
package org.cloudata.core.tablet;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.ScanCell;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.io.CWritable;
import org.cloudata.core.common.io.CWritableUtils;


/**
 * 컬럼의 Cell에 저장되는 데이터(ValueObject)<br>
 * 클라이언트 API와 Tablet Server 간에 데이터 전송을 위해 사용된다.<br>
 * @author 김형준
 *
 */
public class ColumnValue implements CWritable, Comparable<ColumnValue>, org.apache.hadoop.io.Writable {
  public static final Log LOG = LogFactory.getLog(ColumnValue.class.getName());
  
  public static final String ARRAY_CLASS_NAME = (new ColumnValue[0]).getClass().getName();
  public static final int ARRAY_CLASS_LENGTH = ARRAY_CLASS_NAME.getBytes().length;
  public static long NANO_TIME = 0; 
  /**
   * 값의 Row.Key
   */
  protected Row.Key rowKey;
  
  /**
   * Cell.Key
   */
  protected Cell.Key cellKey;
  
  /**
   * Value
   */
  protected byte[] value;
  
  /**
   * timestamp (columnkey가 동일한 경우 timestamp의 desc 순으로 저장된다.)
   */
  protected long timestamp;
  
  /**
   * 삭제 여부(삭제된 데이터는 compaction 작업 중 영구히 삭제된다.
   */
  protected boolean deleted;
  
  /**
   * 해당 Cell.Key에 다른 버전의 데이터 갯수(read 시에만 사용)
   */
  private int numOfValues;
  
  public ColumnValue() {
    rowKey = new Row.Key();
    cellKey = new Cell.Key();
  }
  
  public ColumnValue(Row.Key rowKey, Cell.Key cellKey, byte[] value) {
    this(rowKey, cellKey, value, false, System.currentTimeMillis());
  }
  
  public ColumnValue(Row.Key rowKey, Cell.Key cellKey, byte[] value, boolean deleted) {
    this(rowKey, cellKey, value, deleted, System.currentTimeMillis());
  }
  
  public ColumnValue(Row.Key rowKey, Cell.Key cellKey, byte[] value, long timestamp) {
    this(rowKey, cellKey, value, false, timestamp);
  }
  
  public ColumnValue(Row.Key rowKey, Cell.Key cellKey, byte[] value, boolean deleted, long timestamp) {
    this.rowKey = rowKey;
    this.cellKey = cellKey;
    this.value = value;
    this.deleted = deleted;
    this.timestamp = timestamp;
  }

  public ColumnValue copyColumnValue() {
    try {
      ColumnValue columnValue = new ColumnValue();
      
      byte[] buf = new byte[columnValue.size()];
      
      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bout);
      this.write(out);
      
      buf = bout.toByteArray();
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(buf));
      
      columnValue.readFields(in);
      
      out.close();
      in.close();
      return columnValue;
    } catch (Exception e) {
      LOG.error(e);
      return null;
    }
  }
  
  public boolean isDeleted() {
    return deleted;
  }

  public void setDeleted(boolean deleted) {
    this.deleted = deleted;
  }

  public Row.Key getRowKey() {
    return rowKey;
  }

  public void setRowKey(Row.Key rowKey) {
    this.rowKey = rowKey;
  }

  public Cell.Key getCellKey() {
    return cellKey;
  }

  public void setCellKey(Cell.Key cellKey) {
    this.cellKey = cellKey;
  }

  public byte[] getValue() {
    return value;
  }

  public void setValue(byte[] value) {
    this.value = value;
  }

  public String getValueAsString() {
    if(value == null)   return null;
    return new String(value);
  }
  
  public CWritable getValue(CWritable writable) throws IOException {
    if(value == null)   return null;
    
    DataInputStream din = new DataInputStream(new ByteArrayInputStream(value));
    writable.readFields(din);
    return writable;
  }

  public String toString() {
    return "[Row.Key=" + rowKey + ", CellKey=" + cellKey + 
            ", value=" + getValueAsString() + 
            ", timestamp=" + timestamp + ", deleted=" + deleted + "]";
  }

  public void readFields(DataInput in) throws IOException {
    //long startNano = System.nanoTime();
    rowKey.readFields(in);
    cellKey.readFields(in);
    int opCode = in.readInt();
    if(opCode == Constants.DELETEED) {
      deleted = true;
    } else if(opCode == Constants.INSERTED) {
      deleted = false;
    } else {
      throw new IOException("Wrong record operation code(DELETEED or INSERTED): " + opCode);
    }
    timestamp = in.readLong();
    numOfValues = in.readInt();
    int valueLength = in.readInt();
    //System.out.println(">>>>>" + valueLength);
    value = valueLength < 0 ? null : new byte[valueLength];
    if(value != null) {
      in.readFully(value);
    }  
  }

  public void write(DataOutput out) throws IOException {
    rowKey.write(out);
    
    cellKey.write(out);
    
    //write operation
    if(deleted) {
      out.writeInt(Constants.DELETEED);
    } else {
      out.writeInt(Constants.INSERTED);
    }

    //write timestame
    out.writeLong(timestamp);
    
    //write numOfValues
    out.writeInt(numOfValues);

    //write value
    int valueLength = (value == null ? -1 : value.length);
    
    out.writeInt(valueLength);
    
    if(valueLength > 0) {
      out.write(value);
    }
  }

  public int size() throws IOException {
    //columnKey + deleted + timestamp + valueSize + value
    return  rowKey.getByteSize() + 
             cellKey.getByteSize() + 
             CWritableUtils.getIntByteSize() + 
             CWritableUtils.getLongByteSize() + 
             CWritableUtils.getIntByteSize() + 
             CWritableUtils.getIntByteSize() + 
             (value == null ? 0 : value.length);
  }
  
  public int getAllocatedSize() {
    return 40 + rowKey.getAllocatedSize() + cellKey.getAllocatedSize() + (value == null ? 0 : value.length);
  }
  
  /**
   * 주의사항: 동일한 columnKey내에서는 time으로 desc으로 정렬되도록 한다.(최근 데이터가 가장 위에 올라온다) 
   */
  public int compareTo(ColumnValue obj) {
    //FIXME Timestamp가 밀리세컨드 단위인데 밀리세컨드 단위에서 동일한 레코드가 추가될 경우가 있는지 확인 필요
    //ColumnValue must be desc
    if(rowKey == null) return -1;
    if(obj.rowKey == null) return 1;
    int compare = rowKey.compareTo(obj.rowKey);
    if(compare != 0) {
      return compare;
    }
    
    if(cellKey == null) return -1;
    if(obj.cellKey == null) return 1;
    
    //timestamp 비교는 작은 경우 1, 큰 경우 -1을 반환한다.
    //이유는 ValueCollection에서 version 갯수로 반환을 요청 받은 경우 아래에서부터 처리하기 어렵기 때문이다.
    if((compare = cellKey.compareTo(obj.cellKey)) == 0) {
      if(timestamp == obj.timestamp) {
        if(deleted == obj.deleted) return 0;
        else return deleted ? -1 : 1;
      } else return timestamp > obj.timestamp ? -1 : 1;
    } else return compare;
  }

  public boolean equals(Object obj) {
    if(!(obj instanceof ColumnValue)) {
      return false;
    }
    
    ColumnValue that = (ColumnValue) obj;
    
    if (this.rowKey == that.rowKey 
        || (this.rowKey != null && this.rowKey.equals(that.rowKey))) {
      if (this.cellKey == that.cellKey 
          || (this.cellKey != null && this.cellKey.equals(that.cellKey))) {
        
        return this.timestamp == that.timestamp 
          && this.deleted == that.deleted 
          && this.numOfValues == that.numOfValues; 
        
        // Not compare the value due to performance reason
        // It seems ok to compare all the members except value
        // in order to determine the equivalence of the two objects
        
        // && Arrays.equals(this.value, that.value);
      }
    }
    
    return false;
  }
  
  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public String getPrintValue(String encoding) {
    try {
      String keyStr = null;
      if(cellKey != null || cellKey.getBytes() != null) {
        keyStr = new String(cellKey.getBytes(), 0, cellKey.getLength(), encoding);
      }
      
      String valueStr = null;
      if(value != null) {
        valueStr = new String(value, 0, value.length, encoding);
      }
      
      if(numOfValues > 1) {
        return keyStr + ":" + valueStr + "(" + numOfValues + ")";
      } else {
        return keyStr + ":" + valueStr;
      }
    } catch (UnsupportedEncodingException e) {
      return "Error:" + e.getMessage();
    } catch (Exception e) {
      e.printStackTrace();
      return "";
    }
  }
  
  public String getPrintValue() {
    return getPrintValue("utf-8");
  }

  public boolean equalsKey(ColumnValue columnValue) {
    return rowKey.equals(columnValue.rowKey) &&
    cellKey.equals(columnValue.cellKey);
  }

  public int getNumOfValues() {
    return numOfValues;
  }

  public void setNumOfValues(int numOfValues) {
    this.numOfValues = numOfValues;
  }

  public long getValueAsLong() {
    return Long.parseLong(new String(value));
  }
  
  public int getValueAsInt() {
    return Integer.parseInt(new String(value));
  }

  public Cell.Value copyToCellValue() {
    Cell.Value cellValue = new Cell.Value();
    
    cellValue.setBytes(value);
    cellValue.setTimestamp(timestamp);
    cellValue.setDeleted(deleted);
    cellValue.setNumOfValues(numOfValues);
    
    return cellValue;
  }

  public ScanCell copyToScanCell(String columnName) {
    
    ScanCell scannerCell = new ScanCell();
    scannerCell.setRowKey(rowKey);
    scannerCell.setColumnName(columnName);
    
    scannerCell.setCellKey(cellKey);
    scannerCell.setCellValue(copyToCellValue());
    
    return scannerCell;
  }
}
