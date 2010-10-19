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

import org.cloudata.core.client.Row;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.io.CWritableComparable;
import org.cloudata.core.common.io.CWritableUtils;
import org.cloudata.core.fs.GPath;


/**
 * Value Object which store Tablet' information 
 *
 */
public class TabletInfo implements CWritableComparable {
  private String tableName;
  private String tabletName;
  
  protected Row.Key startRowKey;
  protected Row.Key endRowKey;
  
  String assignedHostName;
  
  public TabletInfo() {
    startRowKey = new Row.Key();
    endRowKey = new Row.Key();
  }
  
  public TabletInfo(  String tableName, String tabletName,
      Row.Key startRowKey, Row.Key endRowKey) {
    this.tableName = tableName;
    this.tabletName = tabletName;
    this.startRowKey = startRowKey;
    this.endRowKey = endRowKey;
  }
  
  @Override
  public boolean equals(Object obj) {
    if(!(obj instanceof TabletInfo))    return false;
    
    TabletInfo otherObj = (TabletInfo)obj;
    return tabletName.equals(otherObj.tabletName);
  }

  
  @Override
  public int hashCode() {
    return tabletName.hashCode();
  }

  public Row.Key getEndRowKey() {
    return endRowKey;
  }
  
  public void setEndRowKey(Row.Key endRowKey) {
    this.endRowKey = endRowKey;
  }
  
  public Row.Key getStartRowKey() {
    return startRowKey;
  }
  
  public void setStartRowKey(Row.Key startRowKey) {
    this.startRowKey = startRowKey;
  }

  public GPath getTabletPath() {
    CloudataConf conf = new CloudataConf();
    return new GPath(conf.get("cloudata.root") + 
        "/table/" + tableName + "/" + tabletName);
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getTabletName() {
    return tabletName;
  }

  public void setTabletName(String tabletName) {
    this.tabletName = tabletName;
  }
  
  public String toString() {
    return "[" + tabletName + "][" + startRowKey + "]~[" + endRowKey + "][" + assignedHostName + "]"; 
  }

  /**
   * 특정 rocky 대해 서비스 여부를 반환한다.
   * @param rowKey
   * @return
   */
  public boolean belongRowRange(Row.Key rowKey) {
    if(rowKey.compareTo(startRowKey) > 0 && rowKey.compareTo(endRowKey) <= 0) {
      return true;
    }
    return false;
  }

  public String getAssignedHostName() {
    return assignedHostName;
  }

  public void setAssignedHostName(String assignedHostName) {
    this.assignedHostName = assignedHostName;
  }

  public void readFields(DataInput in) throws IOException {
    tableName = CWritableUtils.readString(in);
    tabletName = CWritableUtils.readString(in);
    
    startRowKey.readFields(in);
    endRowKey.readFields(in);
    assignedHostName = CWritableUtils.readString(in);
  }
  
  public void readOldFields(DataInput in) throws IOException {
    tableName = CWritableUtils.readString(in);
    tabletName = CWritableUtils.readString(in);
    
    endRowKey.readFields(in);
    assignedHostName = CWritableUtils.readString(in);
  }

  public void write(DataOutput out) throws IOException {
    CWritableUtils.writeString(out, tableName);
    CWritableUtils.writeString(out, tabletName);
    
    startRowKey.write(out);
    endRowKey.write(out);
    CWritableUtils.writeString(out, assignedHostName);
  }

  public boolean isAssigned() {
    return assignedHostName != null && assignedHostName.length() > 0;
  }

  public void readFields(byte[] value) throws IOException {
    DataInputStream din = new DataInputStream(new ByteArrayInputStream(value));
    this.readFields(din);
  }
  
  public byte[] getWriteBytes() throws IOException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    DataOutputStream dout = new DataOutputStream(bout);
    this.write(dout);
    return bout.toByteArray();
  }

  public int compareTo(Object obj) {
    TabletInfo tabletInfo = (TabletInfo)obj;
    return endRowKey.compareTo(tabletInfo.endRowKey);
  }
}

