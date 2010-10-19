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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.commitlog.pipe.AsyncFileWriter;
import org.cloudata.core.commitlog.pipe.PipeConnectionInfo;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.ipc.CRPC;
import org.cloudata.core.tabletserver.CommitLog;


public class CommitLogClient {
  static final Log LOG = LogFactory.getLog(CommitLogClient.class);

  static final byte[] MAGIC_KEY = new byte[] { 0xC, 0xA, 0xF, 0xE, 0xB, 0xA, 0xB, 0xE };

  private SocketChannel sc;
  private InetSocketAddress[] pipeAddressList;
  private RpcUtil rpcUtil;
  
  private DataOutputStream currentStream;
  private DataInputStream checkInputStream;
  private int numWritten;
  private int timeout;
  private int poolExpiration;
  private String[] ipAddressList;
  private boolean closed = true;
  private boolean startWriting = false;
  private Thread currentThread;
  private String pipeKey;
  private static AtomicInteger pipeSeq = new AtomicInteger(0);

  private boolean consistencyCheck = false;

  private volatile long lastTouchedTime;

  List<CommitLog> logList = new ArrayList<CommitLog>();
  ByteArrayOutputStream bos = new ByteArrayOutputStream();

  private String currentTabletName;

  private int verifiedAddressIndex = -1;
  
  // only for test
  public CommitLogClient(CloudataConf conf) throws IOException {
    InetSocketAddress[] rpcAddressList = new InetSocketAddress[3];

    for (int i = 0; i < rpcAddressList.length; i++) {
      rpcAddressList[i] = new InetSocketAddress("127.0.0.1", 57001 + i);
    }

    init(conf, rpcAddressList);
  }

  public CommitLogClient(CloudataConf conf, InetSocketAddress[] addresses)
      throws IOException {
    init(conf, addresses);
  }

  private void init(CloudataConf conf, InetSocketAddress[] addressList)
      throws IOException {
    if (addressList == null || addressList.length == 0) {
      throw new IOException("addresses is null or empty");
    } else {
      for (int i = 0; i < addressList.length; i++) {
        if (addressList[i] == null) {
          throw new IOException("addresses contain null");
        }
      }
    }

    this.timeout = conf.getInt("commitLogServer.pipe.timeout", 10) * 1000; // default
                                                                           // 30
                                                                           // sec
    this.poolExpiration = conf.getInt("commitLogServer.pipe.poolExpiration", 10) * 1000; // default
                                                                                        // 2
                                                                                        // sec
    // 3 is num of replica
    // this.es = Executors.newFixedThreadPool(3);

    this.pipeAddressList = new InetSocketAddress[addressList.length];
    this.ipAddressList = new String[addressList.length];
    this.rpcUtil = new RpcUtil(conf, addressList);

    for (int i = 0; i < addressList.length; i++) {
      pipeAddressList[i] = new InetSocketAddress(addressList[i].getAddress(), (addressList[i]
          .getPort() + CommitLogServer.PORT_DIFF));
      ipAddressList[i] = addressList[i].getAddress().getHostAddress() + ":"
          + (addressList[i].getPort() + CommitLogServer.PORT_DIFF);
    }

    lastTouchedTime = System.currentTimeMillis();
  }

  public String getPipeKey() {
    return pipeKey;
  }

  public void open() throws UnmatchedLogException, CommitLogInterruptedException, IOException {
    pipeKey = generateUniqueKey();

    if (LOG.isDebugEnabled()) {
      String msg = "";
      for (String addr : ipAddressList) {
        msg = msg + addr + ", ";
      }
      LOG.debug("Open new pipe with timeout[" + this.timeout 
          + "], key [" + pipeKey + "], -> " + msg);
    }

    String ret = null;
    Socket s = null;
    try {
      sc = SocketChannel.open();
      
      s = sc.socket();
      s.setTcpNoDelay(true);
      s.setSoTimeout(timeout);
      
      int addressIndex = 0;
      if(verifiedAddressIndex >= 0) {
        addressIndex = verifiedAddressIndex;
      } 
      LOG.debug("open pipe to " + pipeAddressList[addressIndex]);
      sc.connect(pipeAddressList[addressIndex]);
    } catch(ClosedByInterruptException e) {
      internalClose();
      throw new CommitLogInterruptedException();
    } catch(IOException e) {
      throw new CommitLogShutDownException(e);
    }
    
    try {
      byte[] body = buildBody();

      currentStream = new DataOutputStream(new BufferedOutputStream(s.getOutputStream(), 8192));
      checkInputStream = new DataInputStream(s.getInputStream());
      currentStream.write(MAGIC_KEY);
      currentStream.writeInt(0);
      currentStream.writeInt(body.length);
      currentStream.write(body);
      currentStream.flush();

      DataInputStream dis = new DataInputStream(s.getInputStream());
      byte[] key = new byte[MAGIC_KEY.length];
      
      if (dis.read(key) < 0) {
          throw new IOException("Fail to establish pipe connection to " + getPipeAddressListStr());
      }

      if (!Arrays.equals(MAGIC_KEY, key)) {
        throw new IOException("Fail to establish pipe connection to " + getPipeAddressList()
            + "due to wrong magic key");
      }
      int opCode = dis.readInt();
      int replyLength = dis.readInt();

      body = new byte[replyLength];
      dis.read(body);
      ret = new String(body);
    } catch (ClosedByInterruptException e) {
      internalClose();
      throw new CommitLogInterruptedException();
    } catch (IOException e) {
      LOG.warn("Fail to establish pipe to " + getPipeAddressListStr() + " due to " + e, e);
      internalClose();
      throw new IOException("Fail to establish pipe connection to " + getPipeAddressListStr(), e);
    }

    if (!ret.equals(Constants.PIPE_CONNECTED)) {
      internalClose();

      if (ret.equals("Log files are unmatched among commit log servers")) {
        throw new UnmatchedLogException("Log files are unmatched among commit log servers");
      }

      throw new IOException("Fail to establish pipe connection to " + getPipeAddressListStr()
          + ". response : " + ret);
    } else {
      closed = false;
    }
  }

  private String getPipeAddressListStr() {
    String msg = "";
    for (InetSocketAddress addr : pipeAddressList) {
      msg = msg + addr.getHostName() + ":" + addr.getPort() + ", ";
    }

    return msg;
  }

  public InetSocketAddress[] getPipeAddressList() {
    return pipeAddressList;
  }
  
  public void startWriting(int seq, String tabletName) throws CommitLogInterruptedException, IOException {
    if (closed) {
      throw new IOException("Pipe is not yet established");
    }
    
    if (currentThread != null && currentThread != Thread.currentThread()) {
      throw new IOException("Concurrent commit log writing");
    }

    // 만일 이미 닫혀있는 Pipe라면 여기서 read를 통해 체크를 해줘야 한다.
    /*
     * while (checkInputStream.available() > 0) { try { LOG.debug("something in
     * the check input stream. it should be flushed"); int r =
     * checkInputStream.read(); } catch(IOException e) { LOG.info("reopen pipe
     * for id : " + id); internalClose(); open(); break; } }
     */

    if (sc.socket().isConnected() == false) {
      LOG.info("reopen pipe for directory : " + tabletName);
      internalClose();
      open();
    }

    try {
      startWriting = true;
      currentThread = Thread.currentThread();

      currentStream.writeInt(seq); // seq number\
      byte[] tabletNameBytes = tabletName.getBytes();
      currentStream.writeInt(tabletNameBytes.length);
      currentStream.write(tabletNameBytes);

      numWritten += (4 + 4 + tabletName.length());
    } catch (ClosedByInterruptException e) {
      throw new CommitLogInterruptedException();
    }
    
    currentTabletName = tabletName;
  }

  public void append(CommitLog commitLog) throws IOException {
    if (currentThread != null && currentThread != Thread.currentThread()) {
      throw new IOException("Concurrent commit log writing");
    }
    
    logList.add(commitLog);
    numWritten += commitLog.getByteSize();
  }

  public void write(byte[] bytes) {
    write(bytes, 0, bytes.length);
  }

  public void write(byte[] bytes, int offset, int length) {
    bos.write(bytes, offset, length);
    numWritten += length;
  }

  public int commitWriting() throws CommitLogInterruptedException, IOException {
    if (currentThread != null && currentThread != Thread.currentThread()) {
      throw new IOException("Concurrent commit log writing");
    }

    int retCommit = 0;
    int ret = 0;
    try {
      numWritten += 4; // 4 is the length of integer numWritten itself
      ret = numWritten;
      currentStream.writeInt(numWritten);

      // UnitTest를 위한 임시방편으로 이렇게 구현한다.
      for (CommitLog log : logList) {
        log.write(currentStream);
      }
      
     
      if (bos.size() > 0) {
        currentStream.write(bos.toByteArray());
      }

      currentStream.flush();
      
      retCommit = checkInputStream.read();
      long ackTime = checkInputStream.readLong();
      
//      if ((t2 - t1) > 2000) {
//        //LOG.info("TIME REPORT [checkInputStream.read()] takes " + (t2 - t1) + " ms");
//
//        if ((t2 - ackTime) > 2000) {
//          LOG.info("TIME REPORT [too long time to ack] takes " + (t2 - ackTime) + " ms");
//        }
//      }
    } catch (ClosedByInterruptException e) {
      throw new CommitLogInterruptedException();
    } catch (IOException e) {
      // try { Thread.sleep(1000); } catch(InterruptedException ex) { }
      LOG.warn("Exception in commiting commitlog pipeKey[" + pipeKey + "] exception : " + e);

      try {
        rollback();
      } catch (ClosedByInterruptException ex) {
        throw new CommitLogInterruptedException();
      } catch (Exception ex) {
        LOG.warn("Fail to rollback commit log for dir[" + currentTabletName + "] exception : " + e);
      }
      throw e;
    } finally {
      numWritten = 0;
      logList.clear();
      bos.reset();

      startWriting = false;
      currentThread = null;
    }

    if (((byte) (retCommit & 0xff)) != AsyncFileWriter.ACK) {
      try {
        rollback();
      } catch (ClosedByInterruptException ex) {
        throw new CommitLogInterruptedException();
      } catch (IOException e) {
        LOG.warn("Fail to rollback commit log for tablet[" + currentTabletName + "]", e);
      }
      internalClose();
      LOG.warn("Fail to commit. commit ret : [" + retCommit + "], commit log rollback and pipe ["
          + pipeKey + "] is closed");
      throw new IOException("Commiting commitlog is fail.");
    }

    // CommitLogFileChannel에서 실제 파일에 기록 할 때
    // 현재 commit하는 txID에 해당하는 데이터의 총 바이트의 길이를 미리 데이터 앞에 기록한다.
    // 따라서 전체 기록양을 반환할 때는 반드시 4만큼 더해서 반환하도록 한다.
    return 4 + ret;
  }

  public boolean dirExists(String tabletName) throws IOException {
    Method rpcMethod = rpcUtil.buildRpcMethod("dirExists", String.class);

    for (int i = 0; i < rpcUtil.getMultiRpcCount(); i++) {
      Object ret = null;
      try {
        ret = rpcUtil.singleCall(rpcMethod, i, tabletName);
      } catch (IOException e) {
        LOG.warn("fail to check dir [" + tabletName + "] exists in server["
            + rpcUtil.getAddressAt(i) + "]due to " + e + ", but try next server");
        continue;
      }

      boolean result = ret.equals("true");
      if(result) {
        return result;
      }
    }

    return false;
  }

  public void prepareClosing() throws IOException {
    if (closed == false && currentStream != null && checkInputStream != null) {
      currentStream.writeInt(Constants.PIPE_DISCONNECT);
      currentStream.flush();
      int code = checkInputStream.read();
      if (code != Constants.PIPE_DISCONNECT_OK) {
        throw new IOException("Closing code is invalid : " + code);
      }
    }
  }

  public void disconnect() throws IOException {
    internalClose();
  }

  public void close() throws IOException {
    try {
      prepareClosing();
    } finally {
      internalClose();
    }
  }

  private void internalClose() throws IOException {
    try {
      if (sc != null) {
        try {
          sc.close();
        } catch (ClosedByInterruptException e) {
          // throw new CommitLogInterruptedException();
        } finally {
          sc = null;
        }
      }
    } finally {
      closed = true;
      LOG.debug("Pipe for dir : " + currentTabletName + " is closed");
      currentTabletName = null;
    }
  }

  public void removeAllLogs(String dirName) throws IOException {
    Method rpcMethod = rpcUtil.buildRpcMethod("removeAllLogs", String.class);
    rpcUtil.multiParallelCall_v2(rpcMethod, dirName);
  }

  public void removeBackupLogFiles(String dirName) throws IOException {
    Method rpcMethod = rpcUtil.buildRpcMethod("removeBackupLogs", String.class);
    rpcUtil.multiParallelCall_v2(rpcMethod, dirName);
  }

  public void rollback() throws IOException {
    /*
     * by sangchul. make rollback disabled since it is not verified
     */

    /*
     * Method rpcMethod = rpcUtil.buildRpcMethod("rollback", String.class,
     * String.class); rpcUtil.multiSerialCall(rpcMethod, dirName,
     * String.valueOf(rollbackPosition));
     */
  }

  public void backupLogFile(String tabletName) throws IOException {
    Method backupMethod = rpcUtil.buildRpcMethod("backupLogFile", String.class);
    try {
      rpcUtil.multiParallelCall_v2(backupMethod, tabletName);
    } catch (IOException e) {
      LOG.warn("Error in backing up log file", e);

      Method restoreMethod = rpcUtil.buildRpcMethod("restoreLogFile", String.class);

      try {
        rpcUtil.multiParallelCall_v2(restoreMethod, tabletName);
      } catch (IOException ex) {
        LOG.warn("Error in restoring log file", ex);
      }

      throw new IOException("Fail to backup log files in dir : " + tabletName);
    }
  }


  public void mark(String tabletName) throws IOException {
    Method backupMethod = rpcUtil.buildRpcMethod("mark", String.class);
    try {
      Object[] retList = rpcUtil.multiParallelCall_v2(backupMethod, tabletName);
      int i = 0;
      for(; i < retList.length; i++) {
        if (((String) retList[i]).equals("OK") == false) {
          break;
        }
      }

      if (i >= retList.length) {
        return;
      }
    } catch (IOException e) {
      LOG.warn("Error in marking log file, restore it", e);
    }

    // when marking is fail
    Method restoreMethod = rpcUtil.buildRpcMethod("restoreMark", String.class);

    try {
      rpcUtil.multiParallelCall_v2(restoreMethod, tabletName);
    } catch (IOException ex) {
      LOG.warn("Error in restoring log file", ex);
    }

    throw new IOException("Fail to backup log files in dir : " + tabletName);
  }
  
  public TransactionData[] readLastLogFile(String tabletName) throws IOException {
    //return readData(rpcUtil.buildRpcMethod("readLastLogFromMarkedPosition", String.class), tabletName);
    return readDataFrom(rpcUtil.buildRpcMethod("readLastLogFromMarkedPosition", String.class), 0, tabletName);
  }

  public TransactionData[] readAllLogFiles(String tabletName) throws IOException {
    return readData(rpcUtil.buildRpcMethod("readAllLogs", String.class), tabletName);
  }

  private TransactionData[] readData(Method readMethod, String tabletName) throws IOException {
    int index = getReplicaIndexForReading(tabletName);
    Set<Integer> calledIndex = new HashSet<Integer>(); 
    while(true) {
      try {
        calledIndex.add(index);
        TransactionData[] result = readDataFrom(readMethod, index, tabletName);
        verifiedAddressIndex = index;
        return result;
      } catch (Exception e) {
        index = -1;
        int replicaCount = rpcUtil.getMultiRpcCount();
        for(int i = 0; i < replicaCount; i++) {
          if(calledIndex.contains(i)) {
            continue;
          }
          index = i;
          break;
        }
        if(index < 0) {
          throw new IOException("No valid commitlog:" + e.getMessage(), e);
        }
      }
    }
  }

  private TransactionData[] readDataFrom(Method readMethod, int index, String tabletName) throws IOException {
    int port = -1;
    int replicaCount = rpcUtil.getMultiRpcCount();

    for (int count = 0; count < replicaCount; count++) {
      try {
        port = (Integer) rpcUtil.singleCall(readMethod, index, tabletName);
        if (port > 0) {
          break;
        }
      } catch (IOException e) {
        LOG.info("Exception in reading commit log data : " + e);
      }

      if (++index % replicaCount == 0) {
        index = 0;
      }
    }

    if (port < 0) {
      return null;
    }

    SocketChannel socketChannel = SocketChannel.open();
    List<TransactionData> txDataList = null;
    try {
      socketChannel.connect(new InetSocketAddress(rpcUtil.getAddressAt(index), port));
      DataInputStream dis = new DataInputStream(new BufferedInputStream(socketChannel.socket()
          .getInputStream(), 8192));

      txDataList = readTxDataFrom(tabletName, dis);
    } finally {
      socketChannel.socket().close();
      socketChannel.close();
    }

    return txDataList.toArray(new TransactionData[0]);
  }

  static List<TransactionData> readTxDataFrom(DataInputStream dis) throws IOException {
    return  readTxDataFrom(null, dis);
  }
  
  static List<TransactionData> readTxDataFrom(String tabletName, DataInputStream dis) throws IOException {
    List<TransactionData> txDataList = new ArrayList<TransactionData>();
    while (true) {
      int totalByteLength = 0;

      try {
        totalByteLength = dis.readInt();
      } catch (EOFException e) {
        break;
      }

      if (totalByteLength <= 0) {
        LOG.warn("totalByteLength is smaller than zero[" + totalByteLength
            + "]!!, ignore this data," + tabletName);
        break;
      }

      if (totalByteLength >= 5 * 1024 * 1024) {
	LOG.warn("totalByteLength is large than 5MB[" + totalByteLength + "]!!, ignore this data," + tabletName);
        break;
      }
      byte[] bytes = new byte[totalByteLength];
      dis.readFully(bytes);
      txDataList.add(TransactionData.createFrom(bytes));
    }

    return txDataList;
  }

  private int getReplicaIndexForReading(String tabletName) throws IOException {
    Method rpcMethod = rpcUtil.buildRpcMethod("getLogFileSize", String.class);

    long minimumFileSize = Long.MAX_VALUE;
    int replicaIndex = 0;

    for (int i = 0; i < rpcUtil.getMultiRpcCount(); i++) {
      long ret = 0;

      try {
        ret = (Long) rpcUtil.singleCall(rpcMethod, i, tabletName);
      } catch (IOException e) {
        continue;
      }

      if (ret < minimumFileSize) {
        minimumFileSize = ret;
        replicaIndex = i;
      }
    }

    return replicaIndex;
  }

  public long[] getAllLogFileSize(String tabletName) throws IOException {
    Method rpcMethod = rpcUtil.buildRpcMethod("getLogFileSize", String.class);
    long[] ret = new long[rpcUtil.getMultiRpcCount()];

    for (int i = 0; i < rpcUtil.getMultiRpcCount(); i++) {
      try {
        ret[i] = (Long) rpcUtil.singleCall(rpcMethod, i, tabletName);
      } catch (Exception e) {
        LOG.warn("Fail to get the size of log file of dir [" + tabletName + "], addr["
            + pipeAddressList[i] + "]");
        ret[i] = -1;
      }
    }

    return ret;
  }

  public long getLogFileSize(String tabletName) throws IOException {
    long[] sizes = getAllLogFileSize(tabletName);
    long ret = -1;

    for (int i = 0; i < rpcUtil.getMultiRpcCount(); i++) {
      if (sizes[i] < 0) {
        continue;
      }

      if (ret >= 0 && sizes[i] != ret) {
        LOG.warn("Unmatched logs! File sizes are different. size1 : " + sizes[i] + ", size2 : "
            + ret);
      }

      if (ret < sizes[i]) {
        ret = sizes[i];
      }
    }

    if (ret < 0) {
      throw new IOException("Fail to get the size of log file of dir [" + tabletName + "]");
    }

    return ret;
  }

  private byte[] buildBody() throws IOException {
    return PipeConnectionInfo.buildFrom(pipeKey, ipAddressList);
  }

  private String generateUniqueKey() {
    return pipeSeq.getAndIncrement() + "@" + getLocalHostIntAddress();
  }

  // for test methods

  public void test_rollbackFileAtIndex(int index, String tabletName, long position) {
    try {
      Method m = rpcUtil.buildRpcMethod("rollback", String.class, String.class);
      rpcUtil.singleCall(m, index, tabletName, String.valueOf(position));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public boolean isClosed() {
    return closed;
  }
  
  public boolean isWritingStarted() {
    return startWriting;
  }

  public void makeExpired() {
    lastTouchedTime = System.currentTimeMillis() - this.poolExpiration - 1;
  }

  public boolean isExpired() {
    return System.currentTimeMillis() - lastTouchedTime >= this.poolExpiration;
  }

  public CommitLogClient touch() {
    lastTouchedTime = System.currentTimeMillis();
    return this;
  }

  public static void formatLogsAt(CloudataConf conf, InetSocketAddress addr) throws IOException {
    Method method;
    try {
      method = CommitLogServerIF.class.getMethod("format");
      Object target = CRPC.getProxyWithoutVersionChecking(CommitLogServerIF.class,
          CommitLogServerIF.versionID, addr, conf);

      method.invoke(target);
    } catch (Exception e) {
      LOG.error("Error in formatting log servers", e);
      throw new IOException(e);
    }
  }

  public void setConsistencyCheck(boolean flag) {
    this.consistencyCheck = flag;
  }

  public boolean getConsistencyCheck() {
    return this.consistencyCheck;
  }

  public boolean hasInvalidCommitLog(String tabletName) throws IOException {
    Method m = rpcUtil.buildRpcMethod("getLogFileSize", String.class);
    Object[] retList = rpcUtil.multiSerialCall(m, tabletName);

    long size = -1;
    for (int i = 0; i < retList.length; i++) {
      if (size < 0) {
        size = (Long) retList[i];
      } else if ((Long) retList[i] != size) {
        return true;
      }
    }
    return false;
  }

  static int localHostIntAddress = -1;

  private static int getLocalHostIntAddress() {
    if (localHostIntAddress == -1) {
      byte[] address;
      try {
        address = InetAddress.getLocalHost().getAddress();
      } catch (UnknownHostException e) {
        LOG.warn("Fail to get local host address. Set 127.0.0.1");
        localHostIntAddress = 0x7F000001;
        return localHostIntAddress;
      }

      int ret = 0;
      for (int i = address.length - 1; i >= 0; i--) {
        ret = (ret << 8) + (address[i] & 0xff);
      }

      localHostIntAddress = ret;
    }

    return localHostIntAddress;
  }
}
