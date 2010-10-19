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
import java.util.List;
import java.util.Set;

import org.apache.zookeeper.ZooKeeper;
import org.cloudata.core.commitlog.CommitLogStatus;
import org.cloudata.core.common.aop.ProxyObject;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.tabletserver.CommitLog;
import org.cloudata.core.tabletserver.TabletServer;


/**
 * change log client interface
 * @author babokim
 *
 */
public interface CommitLogFileSystemIF {
  public static String ADD_OK = "Y";
  
  public void init(CommitLogFileSystemIF systemIF, CloudataConf conf, 
      TabletServer tabletServer, ZooKeeper zk) throws IOException;
  
  /**
   * CommitLog를 저장하기 위해 오픈한다.
   * @param tabletName
   * @param append
   * @throws IOException
   */
  public void open(String tabletName, boolean writable) throws IOException;
  
  public CommitLogLoader getCommitLogLoader(String tabletName, CommitLogStatus commitLogStatus) throws IOException;
  
  /**
   * CommitLog를 저장한다.
   * @param tabletName
   * @param txId
   * @param seq
   * @param commitLog
   * @throws IOException
   */
  public boolean addCommitLog(String tabletName, String txId, int seq, CommitLog commitLog) throws IOException;
  
  public void finishAdding(String tabletName, String txId) throws IOException;
  /**
   * 특정 Tablet의 모든 change log 파일을 삭제한다.
   * @param tabletName
   * @throws IOException
   */
  public void delete(String tabletName) throws IOException;
  
  /**
   * change log 파일의 존재 여부를 반환한다.
   * @param tabletName
   * @return
   * @throws IOException
   */
  public boolean exists(String tabletName) throws IOException;

  /**
   * 하나의 CommitLog 레코드를 저장한 다음에 호출된다.
   * CommitLog 저장소의 구현에 따라 아무런 작업을 하지 않을 수도 있다.
   * @param tabletName
   * @param append
   * @throws IOException
   */
  public void close(String tabletName, boolean writable) throws IOException;

  /**
   * 여러개의 복사본들 사이에 동기화가 되어 있는지 확인한다.
   * 서로 동기화가 맞지 않는 경우 txid가 가장 낮은 값을 기준으로 sync 작업을 수행한다.
   * 결과 값은 sync의 성공 여부
   * @param tabletName
   * @return
   * @throws IOException
   */
  public CommitLogStatus verifyCommitlog(String tabletName) throws IOException;

  /**
   * MinorCompaction 수행 시작할 때 Commitlog 쪽으로 MinorCompaction 시작 상황을 알려주는 역할
   * @param tabletName
   * @throws IOException
   */
  public void startMinorCompaction(String tabletName) throws IOException;

  /**
   * MinorCompaction 수행 종료될 때 Commitlog 쪽으로 MinorCompaction 종료 상황을 알려주는 역할
   * @param tabletName
   * @throws IOException
   */
  public void endMinorCompaction(String tabletName) throws IOException;

  /**
   * 관리하는 모든 정보를 삭제한다.
   * @throws IOException
   */
  public void format() throws IOException;

  /**
   * 해당 Tablet의 log를 관리하는 서버를 반환한다.
   * @param tabletName
   * @return
   */
  public InetSocketAddress[] getCommitLogServerInfo(String tabletName) throws IOException;

  /**
   * 해당 Tablet의 commit log server로 ping을 보낸다.
   * @param tabletName
   * @return
   * @throws IOException
   */
  public boolean ping(String tabletName) throws IOException;
  
  public void processCommitLogServerFailed(Set<String> failedHostNames, List<String> tabletNames);
}
