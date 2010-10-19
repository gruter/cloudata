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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.cloudata.core.tabletserver.CommitLog;
import org.mortbay.log.Log;


public class TransactionData implements Comparable<TransactionData> {
  int seq;
  byte[] tabletName;
  CommitLog[] logList;

  public static TransactionData createFrom(byte[] bytes) throws IOException {
    TransactionData data = new TransactionData();

    ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
    DataInputStream dis = new DataInputStream(bis);

    data.seq = dis.readInt();
    int tabletNameLength = dis.readInt();

    if (tabletNameLength > 1000000) {
      throw new IOException("id length exceeds million");
    }

    data.tabletName = new byte[tabletNameLength];
    dis.readFully(data.tabletName);

    dis.readInt(); // total number of written bytes for commit logs

    List<CommitLog> logList = new ArrayList<CommitLog>();

    int count = 0;
    while (true) {
      CommitLog log = new CommitLog();

      try {
        log.readFields(dis);
      } catch (EOFException e) {
        break;
      }

      logList.add(log);
      count++;
      if(count > 1000) {
        Log.info(new String(data.tabletName) + " too many data:" + count);
        count = 0;
      }
    }

    data.logList = logList.toArray(new CommitLog[0]);

    return data;
  }

  public int getSeqNo() {
    return seq;
  }

  public byte[] getTabletName() {
    return tabletName;
  }

  public int getCommitLogLength() {
    return logList.length;
  }

  public CommitLog getCommitLogAt(int curLogIndex) {
    if(logList.length == 0) {
      return null;
    }
    return logList[curLogIndex];
  }

  @Override
  public int compareTo(TransactionData o) {
    // TODO Auto-generated method stub
    return 0;
  }
}
