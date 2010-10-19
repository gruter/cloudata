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

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.io.CWritable;
import org.cloudata.core.common.io.CWritableUtils;
import org.cloudata.core.fs.GPath;
import org.cloudata.core.tablet.TabletInfo;


public class CommitLog implements CWritable, Comparable<CommitLog> {
  static final Log LOG = LogFactory.getLog(CommitLog.class);
  public static final long USE_SERVER_TIMESTAMP = -1;
  public static final int MAX_VALUE_SIZE = 5 * 1024 * 1024; //5M;
  protected int operation      ;
  protected String tableName   ;
  protected Row.Key rowKey      ;
  protected String columnName  ;
  protected Cell.Key columnKey;
  protected long timestamp;
  protected byte[] value       ;
  
  protected int writeBytes;
  private static  boolean verbose;
  
  public CommitLog( int operation, String tableName, Row.Key rowKey, 
            String columnName, Cell.Key columnKey, long timestamp, byte[] value) throws IOException {
    if(value != null && value.length > MAX_VALUE_SIZE) {
      throw new IOException("data size is too long.[" + value.length + "]");
    }
    
    this.operation   = operation  ;
    this.tableName   = tableName  ;
    this.rowKey      = rowKey     ;
    this.columnName  = columnName ;
    this.columnKey   = columnKey  ;
    this.timestamp = timestamp;
    this.value       = value      ;   
  }

  public CommitLog() {
    rowKey = new Row.Key();
    columnKey = new Cell.Key();
    columnName = "";
  }
  
  public boolean equals(Object obj) {
    if (obj == null || obj instanceof CommitLog == false) {
      return false;
    }
    
    CommitLog target = (CommitLog) obj;

    return target.timestamp == timestamp &&
            target.operation == operation && 
            Arrays.equals(target.value, value) && 
            target.rowKey.equals(rowKey) &&
            target.columnName.equals(columnName)  && 
            target.columnKey.equals(columnKey);
  }

  public Cell.Key getCellKey() {
    return columnKey;
  }

  public String getColumnName() {
    return columnName;
  }

  public int getOperation() {
    return operation;
  }

  public Row.Key getRowKey() {
    return rowKey;
  }

  public String getTableName() {
    return tableName;
  }

  public byte[] getValue() {
    return value;
  }

  public void write(DataOutput out) throws IOException {
    writeBytes = 0;
    out.writeInt(operation);
    writeBytes += CWritableUtils.getIntByteSize();
    
    rowKey.write(out);
    writeBytes += rowKey.getByteSize(); 
      
    writeBytes += CWritableUtils.writeString(out, columnName);
      
    columnKey.write(out);
    writeBytes += columnKey.getByteSize();
      
    out.writeLong(timestamp);
    writeBytes += CWritableUtils.getLongByteSize();
      
    out.writeInt(value == null ? -1 : value.length);
    writeBytes += CWritableUtils.getIntByteSize();
 
    if (value != null) {
      out.write(value);
      writeBytes += value.length;
    }
  }

  public void readFields(DataInput in) throws IOException {
    operation = in.readInt();
    rowKey.readFields(in);
    columnName = CWritableUtils.readString(in);

    columnKey.readFields(in);
    timestamp = in.readLong();
    int length = in.readInt();

    if(length > MAX_VALUE_SIZE) {
      LOG.error("number of bytes in commitlog exceeds CommitLog.MAX_VALUE_SIZE[" + MAX_VALUE_SIZE + "]");
      throw new IOException("data size is too long.[" + length + "]");
    }

    if(length >= 0) {
      value = new byte[length];
      in.readFully(value);
    }
  }

  public int getByteSize() throws UnsupportedEncodingException {
    if(writeBytes > 0) {
      return writeBytes;
    } else {
      return CWritableUtils.getIntByteSize() +
              rowKey.getByteSize() + 
              CWritableUtils.getIntByteSize() +
              columnName.getBytes("UTF-8").length +
              columnKey.getByteSize() +
              CWritableUtils.getLongByteSize() +
              CWritableUtils.getIntByteSize() +
              (value == null ? 0 : value.length);
    }
  }
  
  public String toString() {
    String strValue = "null";
    if(value != null) {
      strValue = new String(value);
    }
    return "[operation=" + getOperationName() + ", rowKey=" + rowKey + ", columnName=" + columnName + ", columnKey=" + columnKey + 
          ",timestamp=" + timestamp + ",value=" + strValue;
  }
  
  public String getOperationName() {
    if(operation == 1)  return "CREATE_ROW";
    if(operation == 3)  return "ADD_COLUMN_VALUE";
    if(operation == 4)  return "DELETE_COLUMN_VALUE";
    
    return "UNKNOWN OPERATION";
  }

  public long getTimestamp() {
    return timestamp;
  }
    
  public static GPath getCommitLogPath(CloudataConf conf, TabletInfo tabletInfo) {
    return new GPath(conf.get("cloudata.root") + "/commitlog/" + 
        tabletInfo.getTableName() + "/" + tabletInfo.getTabletName() + "/");    
  }

  public static GPath getCommitLogTempPath(CloudataConf conf, TabletInfo tabletInfo, String fileId) {
    return new GPath(conf.get("cloudata.root") + "/temp/commitlog/" + tabletInfo.getTableName() + "/" + fileId);    
  }

  public byte[] getBytes() throws IOException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    this.write(new DataOutputStream(bout));
    return bout.toByteArray();
  }

  @Override
  public int compareTo(CommitLog commitLog) {
    if(!tableName.equals(commitLog.tableName)) {
      return tableName.compareTo(commitLog.tableName);
    }
    if(!rowKey.equals(commitLog.rowKey)) {
      return rowKey.compareTo(commitLog.rowKey);
    }
    
    if(timestamp == commitLog.timestamp) {
      return 0;
    } else if(timestamp > commitLog.timestamp) {
      return 1; 
    } else {
      return -1;
    }
  }
  
  public int hashCode() {
    return (tableName + rowKey.toString() + timestamp).hashCode();
  }
}
