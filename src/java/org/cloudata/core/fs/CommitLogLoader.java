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
package org.cloudata.core.fs;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.commitlog.CommitLogClient;
import org.cloudata.core.commitlog.CommitLogStatus;
import org.cloudata.core.commitlog.TransactionData;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.tabletserver.CommitLog;


/**
 * @author jindolk
 *
 */
public class CommitLogLoader {
  static final Log LOG = LogFactory.getLog(CommitLogLoader.class);
  
  TransactionData[] dataList;

  int curLogIndex = 0;

  int curDataIndex = 0;
  
  boolean inConsistent;
  
  String tabletName;
  
  InetSocketAddress[] addresses;
  
  CloudataConf conf;
  
  int lastSeq = -1;
  
  boolean matchLastSeq = true;
  
  public CommitLogLoader (CloudataConf conf, String tabletName, CommitLogStatus commitLogStatus,
      InetSocketAddress[] addresses) throws IOException {
    this.conf = conf;
    this.tabletName = tabletName;
    this.inConsistent = commitLogStatus.isNeedCompaction();
    this.addresses = addresses;
    
    if(addresses == null || addresses.length == 0) {
      throw new IOException("No commit log server list:" + tabletName);
    }
  }
  
  public CommitLog nextCommitLog() throws IOException {
    if (dataList == null || dataList.length == 0) {
      long startTime = System.currentTimeMillis();
      loadCommitLog();
      if(dataList == null) {
        LOG.info(tabletName + " commit log load time: " + (System.currentTimeMillis() - startTime) + " ms, count:null");
      } else {
        LOG.info(tabletName + " commit log load time: " + (System.currentTimeMillis() - startTime) + " ms, count:" + dataList.length);
      }
      if (dataList == null || dataList.length == 0) {
        return null;
      }
    }

    if (curLogIndex >= dataList[curDataIndex].getCommitLogLength()) {
      curLogIndex = 0;
      curDataIndex++;
    }

    if (curDataIndex >= dataList.length) {
      return null;
    }

    CommitLog ret = dataList[curDataIndex].getCommitLogAt(curLogIndex);
    curLogIndex++;

    return ret;
  }
  
  private void loadCommitLog() throws IOException {
    lastSeq = 0;
    List<TransactionData[]> tDataList = new ArrayList<TransactionData[]>();
    
    int maxLastSeq = 0;
    Exception error = null;
    for(int i = 0; i < addresses.length; i++) {
      CommitLogClient commitLogClient = null;
      try {
        LOG.info(tabletName + " read from commitlogserver:" + addresses[i]);
        commitLogClient = new CommitLogClient(conf, new InetSocketAddress[]{addresses[i]});
        TransactionData[] datas = commitLogClient.readLastLogFile(tabletName);
        if(datas != null && datas.length > 0) {
          tDataList.add(datas);
          
          int dataLastSeq = datas[datas.length - 1].getSeqNo();
          
          if(matchLastSeq && maxLastSeq > 0 && maxLastSeq != dataLastSeq) {
            LOG.info(tabletName + " different commit log max seq:" + maxLastSeq + "," + dataLastSeq);
            matchLastSeq = false;
          }
          
          if(dataLastSeq > maxLastSeq) {
            maxLastSeq = dataLastSeq;
          }
        }
      } catch (Exception e) {
        LOG.warn(tabletName + ".commitLogClient.readLastLogFile error:" + e.getMessage(), e);
        error = e;
      } finally {
        if(commitLogClient != null) {
          try {
            commitLogClient.close();
          } catch (Exception e) {
            LOG.warn(tabletName + ".commitLogClient.close error:" + e.getMessage(), e);
          }
        }
      }
      
      lastSeq = maxLastSeq;
    }
    
    if(tDataList.size() == 0 && error != null) {
      throw new IOException(tabletName + " commit log loda error:" + error.getMessage(), error);
    }
    if(tDataList.size() == 1) {
      dataList = tDataList.get(0);
      return;
    } 

    TransactionData[][] tDatas = tDataList.toArray(new TransactionData[][]{});
    
    List<TransactionData> result = new ArrayList<TransactionData>();
    List<TransactionData> buf = new ArrayList<TransactionData>();
    
    int[] indexs = new int[tDatas.length];
    
    while(true) {
      buf.clear();
      boolean end = true;
      for(int i = 0; i < tDatas.length; i++) {
        if(tDatas[i].length > indexs[i]) {
          buf.add(tDatas[i][indexs[i]]);
          end = false;
        } else {
          buf.add(null);
        }
      }
      
      if(end) {
        break;
      }
      
      Integer[] sameSeqIndex = findMinSeq(buf);
      if(sameSeqIndex == null || sameSeqIndex.length == 0) {
        LOG.warn("findMinSeq return null:" + tabletName);
      }
      TransactionData maxCommitLogData = findMaxCommitLog(buf, sameSeqIndex);
      if(maxCommitLogData != null) {
        result.add(maxCommitLogData);
      }
      for(int i = 0; i < sameSeqIndex.length; i++) {
        indexs[sameSeqIndex[i]] ++;
      }
    }
    
    dataList = result.toArray(new TransactionData[]{});
  }
  
  private TransactionData findMaxCommitLog(List<TransactionData> tDatas, Integer[] sameSeqIndex) {
    int max = 0;
    TransactionData result = null;
    for(int i = 0; i < sameSeqIndex.length; i++) {
      TransactionData data = tDatas.get(sameSeqIndex[i]);
      if(data.getCommitLogLength() > max) {
        if(max > 0) {
          LOG.info(tabletName + " # commit log not mached, seq=" + data.getSeqNo() + ", size1=" + max + ", size2=" + data.getCommitLogLength());
        }
        max = data.getCommitLogLength();
        result = data;
      }
    }
    
    return result;
  }
  
  private Integer[] findMinSeq(List<TransactionData> tDatas) {
    int minSeq = Integer.MAX_VALUE;
    for(TransactionData eachData: tDatas) {
      if(eachData == null) {
        continue;
      }
      if(eachData.getSeqNo() < minSeq) {
        minSeq = eachData.getSeqNo();
      }
    }
    
    List<Integer> result = new ArrayList<Integer>();
    int size = tDatas.size(); 
    for(int i = 0; i < size; i++) {
      TransactionData eachData = tDatas.get(i); 
      if(eachData == null) {
        continue;
      }
      if(eachData.getSeqNo() == minSeq) {
        result.add(i);
      }
    }
    
    return result.toArray(new Integer[]{});
  }
  
  public int getLastSeq() {
    return lastSeq;
  }

  public boolean isMatchLastSeq() {
    return matchLastSeq;
  }
}
