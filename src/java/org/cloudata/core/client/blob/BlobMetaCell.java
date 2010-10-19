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
package org.cloudata.core.client.blob;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.cloudata.core.client.Cell;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.io.CWritable;
import org.cloudata.core.common.io.CWritableUtils;


/**
 * @author jindolk
 *
 */
public class BlobMetaCell implements CWritable {
  private String tableName;
  private Row.Key rowKey;
  private String columnName;
  private Cell.Key cellKey;
  private long offset;
  private long length;
  
  public BlobMetaCell() {
    
  }
  
  public BlobMetaCell(byte[] data) throws IOException {
    readFields(new DataInputStream(new ByteArrayInputStream(data)));
  }

  public BlobMetaCell(String tableName, Row.Key rowKey, String columnName, Cell.Key cellKey) {
    this.tableName = tableName;
    this.rowKey = rowKey;
    this.columnName = columnName;
    this.cellKey = cellKey;
    if(this.cellKey == null) {
      this.cellKey = Cell.Key.EMPTY_KEY;
    }
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    tableName = CWritableUtils.readString(in);
    
    rowKey = new Row.Key();
    rowKey.readFields(in);
    
    columnName = CWritableUtils.readString(in);
    
    cellKey = new Cell.Key();
    cellKey.readFields(in);
    
    length = in.readLong();
    offset = in.readLong();
    
    //deleted = "Y".equals(NWritableUtils.readString(in));
  }

  @Override
  public void write(DataOutput out) throws IOException {
    CWritableUtils.writeString(out, tableName);
    rowKey.write(out);
    CWritableUtils.writeString(out, columnName);
    cellKey.write(out);
    out.writeLong(length);
    out.writeLong(offset);
    //NWritableUtils.writeString(out, deleted ? "Y" : "N");
  }
  
  public byte[] getBytes() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    
    write(new DataOutputStream(out));
    
    return out.toByteArray();
  }
  
  public String toString() {
    return tableName + "," + rowKey.toString() + "," + columnName + "," + cellKey.toString();
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public Row.Key getRowKey() {
    return rowKey;
  }

  public void setRowKey(Row.Key rowKey) {
    this.rowKey = rowKey;
  }

  public String getColumnName() {
    return columnName;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  public Cell.Key getCellKey() {
    return cellKey;
  }

  public void setCellKey(Cell.Key cellKey) {
    this.cellKey = cellKey;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public long getLength() {
    return length;
  }

  public void setLength(long length) {
    this.length = length;
  }
}
