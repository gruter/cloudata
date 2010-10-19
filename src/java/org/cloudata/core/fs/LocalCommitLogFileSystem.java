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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.ZooKeeper;
import org.cloudata.core.commitlog.CommitLogStatus;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.util.FileUtil;
import org.cloudata.core.tabletserver.CommitLog;
import org.cloudata.core.tabletserver.TabletServer;


/**
 * 로컬 파일시스템을 commit log 사용. 
 * 주로 테스트 용도로 활용
 * @author babokim
 *
 */
public class LocalCommitLogFileSystem extends CommitLogFileSystem {
  private String logRootDir;
  private Map<String, DataInputStream> inputs = new HashMap<String, DataInputStream>();
  private Map<String, DataOutputStream> outputs = new HashMap<String, DataOutputStream>();
  
  //tabletName -> CurrentFileName;
  private Map<String, String> currentLogFiles = new HashMap<String, String>();
  
  //tabletName -> CommitLogFile
  private Map<String, List<File>> minorCompactionPaths = new HashMap<String, List<File>>();
  
  public LocalCommitLogFileSystem(CloudataConf conf) {
    super();
    logRootDir = conf.get("commitlog.data.dir");
    if(!logRootDir.endsWith("/")) {
      logRootDir += "/";
    }
  }
  
  public boolean ping(String tabletName) throws IOException {
    return true;
  }
  
  private String parseParth(String src) {
    if(src.startsWith("/")) {
      src = src.substring(1);
    }
    return logRootDir + src;
  }
  
  public void delete(String src) throws IOException {
    File file = new File(parseParth(src));
    System.out.println("delete1:" + file.getAbsolutePath());
    file.delete();
  }

  public boolean exists(String src) throws IOException {
    File file = new File(parseParth(src));
    return file.exists();
  }


  public boolean addCommitLog(String src, String txId, int seq, CommitLog commitLog) throws IOException {
    DataOutputStream out = outputs.get(src);
    commitLog.write(out);
    return true;
  }

  public void close(String src, boolean append) throws IOException {
    if(append) {
      if(!outputs.containsKey(src)) {
        throw new IOException("no opended commitlog file:" + src);
      }
      DataOutputStream out = outputs.remove(src);
      out.close();
    } else {
      if(!inputs.containsKey(src)) {
        throw new IOException("no opended commitlog file:" + src);
      }
      DataInputStream in = inputs.remove(src);
      in.close();
    }    
  }

  public CommitLogLoader getCommitLogLoader(String tabletName, CommitLogStatus commitLogStatus) throws IOException {
    return null;
  }
  
  public void open(String src, boolean append) throws IOException {
    if(append) {
      if(outputs.containsKey(src)) {
        throw new IOException("Commitlog file already opened. first close:" + src);
      }
      File file = new File(parseParth(src));
      if(!file.getParentFile().exists()) {
        file.getParentFile().mkdirs();
      }    
      DataOutputStream out = new DataOutputStream(new FileOutputStream(parseParth(src), append));
      outputs.put(src, out);
    } else {
      if(inputs.containsKey(src)) {
        throw new IOException("Commitlog file already opened. first close:" + src);
      }   
      DataInputStream input = new DataInputStream(new FileInputStream(parseParth(src)));
      inputs.put(src, input);
    }
  }

  public CommitLogStatus verifyCommitlog(String tabletName) throws IOException {
    return null;
  }
  
  public InetSocketAddress[] getCommitLogServerInfo(String tabletName) throws IOException {
    return null;
  }

  public void init(CommitLogFileSystemIF systemIF, CloudataConf conf, 
      TabletServer tabletServer, ZooKeeper zk) throws IOException {
  }

  public void rollback(String tabletName, String txId, 
      InetSocketAddress[] addresses, String errorMessage)  {
    // TODO Auto-generated method stub
  }

  public void processCommitLogServerFailed(Set<String> failedHostNames, List<String> tabletNames) {
  }
  
  public String getTestHandlerKey() {
    // TODO Auto-generated method stub
    return null;
  }

  public List<String> getMaxTxIds(String tabletName) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }
  
  public boolean rollbackSingleServer(String tabletName, String txId, 
      InetSocketAddress addresse) {
    return false;
  }
  
  public void startMinorCompaction(String tabletName) throws IOException {
  }

  public void endMinorCompaction(String tabletName) throws IOException {
  }

  public void format() throws IOException {
    FileUtil.delete(logRootDir, true);    
  }

  public List<CommitLogStatus> getCommitLogStatus(String tabletName) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  public void finishAdding(String tabletName, String txId) throws IOException {
    // TODO Auto-generated method stub

  }
}
