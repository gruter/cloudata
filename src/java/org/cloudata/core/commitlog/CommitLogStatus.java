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
package org.cloudata.core.commitlog;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.cloudata.core.common.io.CWritable;
import org.cloudata.core.common.io.CWritableUtils;


public class CommitLogStatus implements CWritable {
  private String maxTxId;
  private String hostName;
  private int maxSeq;
  private int fileCount;
  private long fileSize;
  
  //네트워크 전송에는 사용하지 않는다.
  private boolean needCompaction = false;
  
  public CommitLogStatus() {
    maxTxId = "";
  }
  
  public int getFileCount() {
    return fileCount;
  }
  public void setFileCount(int fileCount) {
    this.fileCount = fileCount;
  }
  public long getFileSize() {
    return fileSize;
  }
  public void setFileSize(long fileSize) {
    this.fileSize = fileSize;
  }

  public int getMaxSeq() {
    return maxSeq;
  }
  public void setMaxSeq(int maxSeq) {
    this.maxSeq = maxSeq;
  }
  public String getMaxTxId() {
    return maxTxId;
  }
  public void setMaxTxId(String maxTxId) {
    this.maxTxId = maxTxId;
  }

  public String getHostName() {
    return hostName;
  }

  public void setHostName(String hostName) {
    this.hostName = hostName;
  }
  
  public String toString() {
    return "maxTxId=" + maxTxId + ",maxSeq=" + maxSeq + 
            ",fileCount=" + fileCount +
            ",fileSize=" + fileSize +
            ", hostName = " + hostName; 
  }
  
  public boolean equals(Object obj) {
    if(!(obj instanceof CommitLogStatus)) {
      return false;
    }
    
    CommitLogStatus other = (CommitLogStatus)obj;
    
    return fileCount == other.fileCount &&
      maxSeq == other.maxSeq &&
      fileSize == other.fileSize &&
      maxTxId.equals(other.maxTxId);
  }
  
  public void readFields(DataInput in) throws IOException {
    maxTxId = CWritableUtils.readString(in);
    hostName = CWritableUtils.readString(in);
    maxSeq = in.readInt();
    fileCount = in.readInt();
    fileSize = in.readLong();
  }
  
  public void write(DataOutput out) throws IOException {
    CWritableUtils.writeString(out, maxTxId);    
    CWritableUtils.writeString(out, hostName);    
    out.writeInt(maxSeq);
    out.writeInt(fileCount);
    out.writeLong(fileSize);
  }

  public boolean isNeedCompaction() {
    return needCompaction;
  }

  public void setNeedCompaction(boolean needCompaction) {
    this.needCompaction = needCompaction;
  }

}
