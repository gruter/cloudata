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
import java.util.Comparator;

import org.cloudata.core.common.io.CWritable;
import org.cloudata.core.common.io.CWritableUtils;


public class TabletServerInfo implements CWritable, Comparable<TabletServerInfo> {
  private String hostName;
  private long lastHeartbeatTime;
  private int numOfTablets;
  
  public TabletServerInfo() {
    
  }
  
  public TabletServerInfo(String hostName) {
    this.hostName = hostName;
  }

  public long getLastHeartbeatTime() {
    return lastHeartbeatTime;
  }

  public void setLastHeartbeatTime(long lastHeartbeatTime) {
    this.lastHeartbeatTime = lastHeartbeatTime;
  }

  public String getHostName() {
    return hostName;
  }
  
  @Override
  public boolean equals(Object obj) {
    if( !(obj instanceof TabletServerInfo) )  return false;
    
    TabletServerInfo otherTabletServerInfo = (TabletServerInfo)obj;
    return hostName.equals(otherTabletServerInfo.hostName);
  }
  
  @Override
  public int hashCode() {
    return hostName.hashCode();
  }
  
  public void readFields(DataInput in) throws IOException {
    hostName = CWritableUtils.readString(in);
    lastHeartbeatTime = in.readLong();
    numOfTablets = in.readInt();
  }

  public void write(DataOutput out) throws IOException {
    CWritableUtils.writeString(out, hostName);
    out.writeLong(lastHeartbeatTime);
    out.writeInt(numOfTablets);
  }
  
  public String toString() {
    return hostName + "," + lastHeartbeatTime;
  }

  public int getNumOfTablets() {
    return numOfTablets;
  }

  public void setNumOfTablets(int numOfTablets) {
    this.numOfTablets = numOfTablets;
  }
  
  public static class TabletServerInfoComparator implements Comparator<TabletServerInfo> {
    public int compare(TabletServerInfo tabletServerInfo1, TabletServerInfo tabletServerInfo2) {
      if(tabletServerInfo1.numOfTablets > tabletServerInfo2.numOfTablets) {
        return 1;
      } else if(tabletServerInfo1.numOfTablets == tabletServerInfo2.numOfTablets) {
        return 0;
      } else {
        return -1;
      }
    }
    
  }

  public void addNumOfTablets() {
    synchronized(this) {
      numOfTablets++;
    }
  }
  
  public void subtractNumOfTablets() {
    synchronized(this) {
      numOfTablets--;
    }
  }

  public int compareTo(TabletServerInfo o) {
    return hostName.compareTo(o.hostName);
  }
}
