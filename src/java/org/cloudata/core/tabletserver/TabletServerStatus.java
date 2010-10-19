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

import org.cloudata.core.common.io.CWritable;
import org.cloudata.core.common.io.CWritableUtils;


public class TabletServerStatus implements CWritable {
  long freeMemory;
  long totalMemory;
  long maxMemory;
  long serverStartTime;
  
  long memoryCacheSize;
  
  int numMajorThread;
  int numSplitThread;
  int numEtcThread;
  
  int commitLogPipe;
  
  String[] txStatistics;

  public TabletServerStatus() {
    
  }

  public int getCommitLogPipe() {
    return commitLogPipe;
  }

  public void setCommitLogPipe(int commitLogPipe) {
    this.commitLogPipe = commitLogPipe;
  }
  
  public long getFreeMemory() {
    return freeMemory;
  }

  public void setFreeMemory(long freeMemory) {
    this.freeMemory = freeMemory;
  }

  public long getMaxMemory() {
    return maxMemory;
  }

  public void setMaxMemory(long maxMemory) {
    this.maxMemory = maxMemory;
  }

  public long getTotalMemory() {
    return totalMemory;
  }

  public void setTotalMemory(long totalMemory) {
    this.totalMemory = totalMemory;
  }

  public void readFields(DataInput in) throws IOException {
    freeMemory = in.readLong();
    totalMemory = in.readLong();
    maxMemory = in.readLong();
    serverStartTime = in.readLong();
    memoryCacheSize = in.readLong();
    
    numMajorThread = in.readInt();
    numSplitThread = in.readInt();
    numEtcThread = in.readInt();
    
    commitLogPipe = in.readInt();
    
    txStatistics = CWritableUtils.readStringArray(in);
  }

  public void write(DataOutput out) throws IOException {
    out.writeLong(freeMemory);
    out.writeLong(totalMemory);
    out.writeLong(maxMemory);
    out.writeLong(serverStartTime);
    out.writeLong(memoryCacheSize);
    
    out.writeInt(numMajorThread);
    out.writeInt(numSplitThread);
    out.writeInt(numEtcThread);
    
    out.writeInt(commitLogPipe);
    
    CWritableUtils.writeStringArray(out, txStatistics);
  }

  public long getMemoryCacheSize() {
    return memoryCacheSize;
  }

  public void setMemoryCacheSize(long memoryCacheSize) {
    this.memoryCacheSize = memoryCacheSize;
  }

  public long getServerStartTime() {
    return serverStartTime;
  }

  public void setServerStartTime(long serverStartTime) {
    this.serverStartTime = serverStartTime;
  }

  public int getNumMajorThread() {
    return numMajorThread;
  }

  public void setNumMajorThread(int numMajorThread) {
    this.numMajorThread = numMajorThread;
  }

  public int getNumSplitThread() {
    return numSplitThread;
  }

  public void setNumSplitThread(int numSplitThread) {
    this.numSplitThread = numSplitThread;
  }

  public int getNumEtcThread() {
    return numEtcThread;
  }

  public void setNumEtcThread(int numEtcThread) {
    this.numEtcThread = numEtcThread;
  }

  public void setTxStatistics(String[] txStatistics) {
    this.txStatistics = txStatistics;
  }

  public String[] getTxStatistics() {
    return txStatistics;
  }

}
