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

import java.io.IOException;
import java.net.UnknownHostException;

import org.cloudata.core.common.ipc.CVersionedProtocol;
import org.cloudata.core.fs.DiskInfo;


interface ProxyInterface {
  public void setThisProxy(ProxyInterface proxyIF);
}

public interface CommitLogServerIF extends CVersionedProtocol {
  static final long versionID = 1L;
  static final boolean IS_TEST_MODE = false;

  static final long REPORT_TIME = 30000;

  public String removeAllLogs(String dirName) throws IOException;
  public long getLogFileSize(String dirName);
  public int readLastLog(String dirName);
  public int readAllLogs(String dirName);
  public String backupLogFile(String dirName);
  public void restoreLogFile(String dirName);
  public void removeBackupLogs(String dirName) throws IOException;
  public void format();
  public void stopCommitLogServer();
  public String dirExists(String dirName);
  public void start() throws IOException;
  public long getChecksum(String dirName) throws IOException;
  public String rollback(String dirName, String position) throws IOException;
  public String mark(String dirName);
  public void restoreMark(String dirName);
  public int readLastLogFromMarkedPosition(String dirName);
  public void test();
  public String getHostInfo() throws UnknownHostException;
  public DiskInfo getDiskInfo() throws IOException;
  public ServerMonitorInfo getServerMonitorInfo() throws IOException;
}
