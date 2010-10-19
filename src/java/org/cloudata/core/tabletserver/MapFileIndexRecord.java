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
package org.cloudata.core.tabletserver;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.cloudata.core.client.Cell;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.io.CWritableComparator;
import org.cloudata.core.common.io.CWritableUtils;
import org.cloudata.core.tablet.ColumnValue;


public class MapFileIndexRecord implements Comparable<MapFileIndexRecord> {
  //private Row.Key startRowKey;
  private byte[] rowKey;
  private byte[] cellKey;
  private long timestamp;
  private long offset;

  public MapFileIndexRecord() {
  }
  
  /**
   * IndexRecord 비교 대상이 되는 객체 생성을 위한 클래스
   * @param endRowKey
   * @param columnKey
   */
  public MapFileIndexRecord(Row.Key rowKey, Cell.Key cellKey) {
    this.rowKey = rowKey.getBytes();
    this.cellKey = cellKey.getBytes();
    this.timestamp = Long.MAX_VALUE;
  }
  
  public MapFileIndexRecord(Row.Key rowKey, Cell.Key columnKey, long timestamp, long fileOffset) {
    this.rowKey = rowKey.getBytes();
    this.cellKey = columnKey.getBytes();
    this.timestamp = timestamp;
    this.offset = fileOffset;    
  }
  
  public MapFileIndexRecord(Row.Key rowKey, ColumnValue columnValue, long fileOffset) {
    this.rowKey = rowKey.getBytes();
    this.cellKey = columnValue.getCellKey().getBytes();
    this.timestamp = columnValue.getTimestamp();
    this.offset = fileOffset;
  }

  public void setValue(byte[] rowKey, byte[] cellKey, long timestamp, long offset) {
    this.rowKey = rowKey;
    this.cellKey = cellKey;
    this.timestamp = timestamp;
    this.offset = offset;
  }
  
  public long getOffset() {
    return offset;
  }
  
  public long getTimestamp() {
    return timestamp;
  }
  
  public Row.Key getRowKey() {
    if(rowKey == null) {
      return null;
    }
    return new Row.Key(rowKey);
  }
  
  public Cell.Key getCellKey() {
    if(cellKey == null) {
      return null;
    }
    return new Cell.Key(cellKey);
  }

  public void write(DataOutput out) throws IOException {
//    rowKey.write(out);
//    columnKey.write(out);
    CWritableUtils.writeVInt(out, rowKey.length);
    out.write(rowKey, 0, rowKey.length);    
    CWritableUtils.writeVInt(out, cellKey.length);
    out.write(cellKey, 0, cellKey.length);    

    out.writeLong(timestamp);
    out.writeLong(offset);
  }
  
  public void read(DataInput in) throws IOException {
//    rowKey.readFields(in);
//    columnKey.readFields(in);
    int length = CWritableUtils.readVInt(in);
    rowKey = new byte[length];
    in.readFully(rowKey, 0, length);
    
    length = CWritableUtils.readVInt(in);
    cellKey = new byte[length];
    in.readFully(cellKey, 0, length);
    
    timestamp = in.readLong();
    offset = in.readLong();
  }

  public int compareTo(MapFileIndexRecord otherIndexRecord) {
    if (this == otherIndexRecord) {
      return 0;
    }
    
    int rowKeyCompare = equalsBytes(rowKey, otherIndexRecord.rowKey);
    
    if(rowKeyCompare == 0) {
      int columnKeyCompare = equalsBytes(cellKey, otherIndexRecord.cellKey);
      
      if(columnKeyCompare == 0) {
        if(timestamp == otherIndexRecord.timestamp) {
          return 0;
        } else if(timestamp < otherIndexRecord.timestamp) {
          //주의: timestamp는 높은 값(최신값)이 map 파일내에서 앞부분에 위치한다. 
          return 1;
        } else {
          return -1;
        }
      } else {
        return columnKeyCompare;
      }
    } else {
      return rowKeyCompare;
    }
  }

  public boolean equals(Object obj) {
    if( !(obj instanceof MapFileIndexRecord) ) {
      return false;
    }
    
    return compareTo((MapFileIndexRecord)obj) == 0;
  }
  
  public int equalsBytes(byte[] bytes1, byte[] bytes2) {
    return CWritableComparator.compareBytes(bytes1, 0, bytes1.length, bytes2, 0, bytes2.length);
  }
  
  public String toString() {
    return new String(rowKey) + "," + new String(cellKey) + "," +
          timestamp + "," + offset;
  }

  public long getMemorySize() throws IOException {
    return CWritableUtils.getVIntSize(rowKey.length) + rowKey.length + 
                        CWritableUtils.getVIntSize(cellKey.length) + cellKey.length + 
                        CWritableUtils.getLongByteSize() + CWritableUtils.getLongByteSize();
  }
}
