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

import org.apache.hadoop.mapred.InputSplit;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.RowFilter;
import org.cloudata.core.common.io.CWritableUtils;

/**
 * InputSplit for Table
 *
 */
public class TableSplit implements InputSplit {
  private Row.Key jobStartRowKey;
  private InputTableInfo inputTableInfo;
  private String hostName;
  private RowFilter rowFilter;
  
  public TableSplit() {
    inputTableInfo = new InputTableInfo();
    hostName = "";
    rowFilter = new RowFilter();
    jobStartRowKey = new Row.Key();
  }

  public TableSplit(InputTableInfo inputTableInfo,
      RowFilter rowFilter) {
    this.inputTableInfo = inputTableInfo;
    this.rowFilter = rowFilter;
  }

  public TableSplit(InputTableInfo inputTableInfo,
      String hostName,
      RowFilter rowFilter) {
    this.inputTableInfo = inputTableInfo;
    this.hostName = hostName;
    this.rowFilter = rowFilter;
  }
  
  public InputTableInfo getInputTableInfo() {
    return inputTableInfo;
  }

  @Override
  public long getLength() {
    return 0;
  }

  @Override
  public String[] getLocations() {
    //FIXME Tablet을 서비스하는 서버로 보내는 것이 좋다.
    //하지만 현재의 hadoop 버전에서는 Tablet 관련 파일이 하나의 Node에 있지 않기 때문에
    //locality 보장이 어렵다.
    if(hostName != null && hostName.length() > 0) {
      return new String[] { hostName };
    } else {
      return new String[] { };
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    inputTableInfo.readFields(in);
    hostName = CWritableUtils.readString(in);
    rowFilter.readFields(in);
    jobStartRowKey.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    inputTableInfo.write(out);
    CWritableUtils.writeString(out, hostName);
    rowFilter.write(out);
    jobStartRowKey.write(out);
  }

  @Override
  public String toString() {
    return inputTableInfo.toString() + ":" + rowFilter.toString();
  }

  public RowFilter getRowFilter() {
    return this.rowFilter;
  }
  
  public Row.Key getJobStartRowKey() {
    return jobStartRowKey;
  }
  
  public void setJobStartRowKey(Row.Key rowKey) {
    jobStartRowKey = rowKey;
  }
}
