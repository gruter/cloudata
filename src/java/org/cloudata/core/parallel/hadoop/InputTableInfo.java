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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.cloudata.core.client.RowFilter;

/**
 * @author jindolk
 *
 */
public class InputTableInfo implements Writable{
  String tableName;
  RowFilter rowFilter;
  
  String joinTableName;
  RowFilter joinRowFilter;
  
  boolean rowScan = true;
  
  String mergeEvaluatorClass;
  
  public InputTableInfo() {
    tableName = "";
    
    joinTableName = "";
    
    mergeEvaluatorClass = "";
    
    rowFilter = new RowFilter();
    
    joinRowFilter = new RowFilter();
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    tableName = WritableUtils.readString(in); 
    rowFilter.readFields(in);
    
    joinTableName = WritableUtils.readString(in); 
    joinRowFilter.readFields(in); 
    
    rowScan = "Y".equals(WritableUtils.readString(in));
    
    mergeEvaluatorClass = WritableUtils.readString(in); 
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeString(out, tableName); 
    rowFilter.write(out);
    
    WritableUtils.writeString(out, joinTableName); 
    joinRowFilter.write(out);
    
    if(rowScan) {
      WritableUtils.writeString(out, "Y"); 
    } else {
      WritableUtils.writeString(out, "N");
    }
    
    WritableUtils.writeString(out, mergeEvaluatorClass);
  }

  public String getTableName() {
    return tableName;
  }

  public String getJoinTableName() {
    return joinTableName;
  }

  public RowFilter getRowFilter() {
    return rowFilter;
  }

  public void setTable(String tableName , RowFilter rowFilter) {
    this.tableName = tableName;
    this.rowFilter = rowFilter;
  }

  public RowFilter getJoinRowFilter() {
    return joinRowFilter;
  }

  public void setJoinTable(String joinTableName, RowFilter joinRowFilter) {
    this.joinTableName = joinTableName;
    this.joinRowFilter = joinRowFilter;
  }
  
  public String toString() {
    if(joinTableName != null && joinTableName.length() > 0) {
      return tableName + " join " + joinTableName;
    } else {
      return tableName;
    }
  }

  public boolean isRowScan() {
    return rowScan;
  }

  public void setRowScan(boolean rowScan) {
    this.rowScan = rowScan;
  }

  public String getMergeEvaluatorClass() {
    return mergeEvaluatorClass;
  }

  public void setMergeEvaluatorClass(String mergeEvaluatorClass) {
    this.mergeEvaluatorClass = mergeEvaluatorClass;
  }
}
