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
package org.cloudata.core.commitlog.pipe;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.cloudata.core.commitlog.CommitLogWritable;
import org.cloudata.core.commitlog.CommitLogWritableUtils;
import org.cloudata.core.common.io.CWritableUtils;


public class PipeConnectionInfo implements CommitLogWritable {
  static final int CHECK_COMMIT_LOG_CONSISTENCY = 0xabcd;
  
  String pipeKey;
  String[] ipAddressList;

  public void readFields(ByteBuffer buf) throws IOException {
    pipeKey = CommitLogWritableUtils.readString(buf);
    ipAddressList = CommitLogWritableUtils.readStringArray(buf);
  }
  
  public void write(ByteBuffer buf) throws IOException {
    throw new IOException("NOT IMPLEMENTED");
  }

  public String nextAddress(int index) {
    return index + 1 >= ipAddressList.length ? null : ipAddressList[index + 1];
  }

  public int find(String ip) {
    for (int i = 0; i < ipAddressList.length; i++) {
      // LOG.debug("ipAddressList[" + i + "] : " + ipAddressList[i]);
      if (ipAddressList[i].equals(ip)) {
        return i;
      }
    }

    if (ip.startsWith("127")) {
      return -1;
    }

    return find("127.0.0.1:" + ip.substring(ip.indexOf(":") + 1));
  }

  public String getPipeKey() {
    return pipeKey;
  }

  public static PipeConnectionInfo decode(ByteBuffer buf) throws IOException {
    PipeConnectionInfo ret = new PipeConnectionInfo();
    ret.readFields(buf);
    return ret;
  }

  public static byte[] buildFrom(String pipeKey, String[] ipAddressList) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);

    CWritableUtils.writeString(dos, pipeKey);
    CWritableUtils.writeStringArray(dos, ipAddressList);

    return bos.toByteArray();
  }
}