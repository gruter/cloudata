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
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.cloudata.core.commitlog.metrics.CommitLogServerMetrics;
import org.cloudata.core.commitlog.pipe.BufferPool;
import org.cloudata.core.commitlog.pipe.CommitLogFileChannel;
import org.cloudata.core.commitlog.pipe.Pipe;
import org.cloudata.core.commitlog.pipe.PipeEventHandler;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.ipc.CRPC;
import org.cloudata.core.common.ipc.CRPC.Server;
import org.cloudata.core.common.lock.LockUtil;
import org.cloudata.core.common.testhelper.FaultInjectionProxy;
import org.cloudata.core.common.testhelper.FaultManager;
import org.cloudata.core.common.util.NetworkUtil;
import org.cloudata.core.fs.CloudataFileSystem;
import org.cloudata.core.fs.DF;
import org.cloudata.core.fs.DiskInfo;
import org.cloudata.core.fs.DiskUsage;
import org.cloudata.core.master.CloudataMaster;
import org.cloudata.core.master.TabletMasterProtocol;


public class CommitLogServer implements CommitLogServerIF, Watcher {
  public static boolean IS_TEST_MODE = false;
  public static final int TIMEOUT = 10000; // 10 sec
  static final Log LOG = LogFactory.getLog(CommitLogServer.class);
  static final int numProcessors = Runtime.getRuntime().availableProcessors();
  public static final int PORT_DIFF = 10;
  static FaultManager fm;

  final Listener listener;
  final Server server;
  final Map<String, Pipe> establishedPipeMap = new HashMap<String, Pipe>();
  final File commitLogStoreFile;
  final CloudataConf conf;

  String lockPath;
  ZooKeeper zk;
  int rpcPort;

  private DF df;
  private DiskUsage du;
  private long serverStartTime;
  private String hostName;
  
  private HeartbeatThread heartbeatThread;
  
  public static CommitLogServerMetrics metrics;
  
  public CommitLogServer(CloudataConf conf) throws IOException {
    CloudataFileSystem fs = CloudataFileSystem.get(conf);
    if(fs == null || !fs.isReady()) {
      LOG.fatal("FileSystem is not ready. CloudataMaster shutdown");
      stopCommitLogServer();
    }
    this.conf = conf;
    IS_TEST_MODE = conf.getBoolean("testmode", false);
    rpcPort = conf.getInt("cloudata.commitlog.server.port", 57001);
    int handlerCount = conf.getInt("commitLogServer.handler.count", numProcessors * 3);
    String commitLogStorePath = getLogStorePath(conf);

    hostName = getHostInfo() + ":" + rpcPort;
    
    initLockService(conf, hostName);

    listener = new Listener(conf, commitLogStorePath);
    commitLogStoreFile = new File(commitLogStorePath);
    server = CRPC.getServer(zk, this, "0.0.0.0", rpcPort, handlerCount, false, conf);
    
    serverStartTime = System.currentTimeMillis();
    
    heartbeatThread = new HeartbeatThread();
    heartbeatThread.setDaemon(true);
    heartbeatThread.start();
    
    metrics = new CommitLogServerMetrics(conf, hostName);
  }

  public String getHostName() {
    return hostName;
  }
  
  public String getHostInfo() throws UnknownHostException {
    try {
      String hostName = InetAddress.getLocalHost().getHostName();
      InetAddress.getByName(hostName);
      return hostName;
    } catch (UnknownHostException e) {
      return InetAddress.getLocalHost().getHostAddress();
    }
  }

  private String getLogStorePath(CloudataConf conf) throws IOException {
    String path = conf.get("commitlog.data.dir"
        , System.getProperty("user.home") + File.separator + ".commitlog");

    String[] pathElements;
    if ((pathElements = path.split("\\$USER_HOME\\$")).length == 1) {
      path = pathElements[0];
    } else if (pathElements.length == 2) {
      path = pathElements[0] + System.getProperty("user.home") + pathElements[1];
    }

    if (!path.endsWith("/")) {
      path = path + "/";
    }
    
    path += "coludata_commitlog";

    File dir = new File(path);

    if (dir.exists()) {
      if (dir.canWrite() == false) {
        throw new IOException("No permission to write commitlog in path [" + path + "]");
	  }
    } else {
      if (dir.mkdirs() == false) {
        throw new IOException("Fail to make commitlog directory[" + path + "]");
      }
    }

    du = new DiskUsage(new File(path), conf);
    
    df = new DF(new File(path), conf);
    df.start();
    return path;
  }
  
  private void deleteLock() {
    try {
      LockUtil.delete(zk, LockUtil.getZKPath(conf, lockPath), true);
    } catch (Exception e) {
      LOG.error("fail to delete node lockpaht [" + lockPath + "]", e);
    }
  }

  @Override
  public void process(WatchedEvent event) {
    if (event.getType() == Event.EventType.None) {
      switch (event.getState()) {
      case SyncConnected:
        break;
      case Disconnected:
	LOG.warn("Disconnected:" + event);
	break;
      case Expired:
        LOG.info("Shutdown cause lock expired:" + event);
        stopCommitLogServer();
      }
    }
  }
  
  private void initLockService(CloudataConf conf, String hostName) throws IOException {
    lockPath = Constants.COMMITLOG_SERVER + "/" + hostName;
    String owner = "CommitLogServer:" + hostName;

    LOG.info("Create node of ZooKeeper lock at [" + LockUtil.getZKPath(conf, lockPath) + "], service type ["
        + conf.get("cloudata.cluster.name", "cloudata") + "], lock owner [" + owner + "]");

    zk = LockUtil.getZooKeeper(conf, owner, this);

    LockUtil.createNodes(zk, LockUtil.getZKPath(conf, lockPath), null, CreateMode.EPHEMERAL);
  }

  public void start() throws IOException {
    server.start();
    listener.start();
  }

  public void stop() {
    try {
      BufferPool.singleton().stop();
  
      synchronized (establishedPipeMap) {
        try {
          for (String eachKey : establishedPipeMap.keySet()) {
            Pipe pipe = establishedPipeMap.get(eachKey);
            if(pipe != null) {
              pipe.close();
            }
          }
        } catch (Exception e) {
          LOG.error("Error while stoping:" + e.getMessage(), e); 
        }
  
        try {
          Thread.sleep(1000);
        } catch (Exception e) {
        }
  
        establishedPipeMap.clear();
      }
      if(listener != null) {
        listener.stopListening();
      }
  
      if(server != null) {
        server.stop();
      }
      
      if(zk != null) {
        try {
          zk.close();
        } catch (InterruptedException e) {
        }
      }
      
      if(metrics != null) {
        metrics.shutdown();
      }
    } catch (Exception e) {
      LOG.error("Error while stoping:" + e.getMessage(), e);
    }
//    ThreadGroup threadGroup = Thread.currentThread().getThreadGroup();
//    Thread[] threads = new Thread[threadGroup.activeCount()];
//    threadGroup.enumerate(threads, true);
//
//    for (Thread thread : threads) {
//      try {
//        thread.interrupt();
//      } catch (Exception e) {
//      }
//    }
  }

  public int getPort() {
    return listener.getPipePort();
  }

  public static void formatAll() throws IOException {
    LOG.info("format all the meta info of commit log servers");

    CloudataConf conf = new CloudataConf();
    ZooKeeper zk = LockUtil.getZooKeeper(conf, "CommitLogServver", null);
    try {
      if (zk.exists(LockUtil.getZKPath(conf, Constants.COMMITLOG_SERVER), false) == null) {
        // 아무것도 없는 경우
        LOG.debug("no lock node");
      } else {
        LockUtil.delete(zk, LockUtil.getZKPath(conf, Constants.COMMITLOG_SERVER), true);
      }

      LOG.info("CommitLog server formated");
    } catch (Exception e) {
      e.printStackTrace();
      LOG.error(e.getMessage(), e);
    } finally {
//      try {
//        zk.close();
//      } catch (InterruptedException e) {
//      }
    }
  }

  public static void main(String[] args) {
    if (args.length > 0 && "-format".equals(args[0])) {
      try {
        formatAll();
      } catch (IOException e) {
        e.printStackTrace();
      }
      System.exit(0);
    }

    CommitLogServerIF server;

    CloudataConf conf = new CloudataConf();

    if (args.length > 0) {
      conf.set("cloudata.commitlog.server.port", conf.getInt("cloudata.commitlog.server.port", 57001)
          + Integer.valueOf(args[0]));
      if ((args.length > 1 && args[1].equalsIgnoreCase("true")) 
          || conf.get("testmode").equalsIgnoreCase("true")) {
        CommitLogServer.IS_TEST_MODE = true;
      }
    }

    try {
      server = CommitLogServer.create(conf);
      server.start();
      LOG.info("Commit log server is ready... ");
    } catch (IOException e) {
      LOG.error("Initializing commit log server is fail", e);
      System.exit(-1);
    }
  }

  public static CommitLogServerIF create(CloudataConf conf) throws IOException {
    CommitLogServerIF server;

    if (CommitLogServer.IS_TEST_MODE) {
      fm = FaultManager.create(conf);
      server = FaultInjectionProxy.wrap(new CommitLogServer(conf), CommitLogServerIF.class);
    } else {
      server = new CommitLogServer(conf);
    }

    return server;
  }

  class HeartbeatThread extends Thread {
    public void run() {
      while(true) {
        try {
          String masterHostName = CloudataMaster.getMasterServerHostName(conf, zk);
          TabletMasterProtocol masterServer = (TabletMasterProtocol) CRPC.getProxy(TabletMasterProtocol.class,
                TabletMasterProtocol.versionID, NetworkUtil.getAddress(masterHostName), conf);
          ServerMonitorInfo info = getServerMonitorInfo();
          metrics.setCurrentTabletNum(info.logFiles.size());
          metrics.setDiskInfo(info.diskInfo);
          masterServer.heartbeatCS(hostName, info);
        } catch (Exception e) {
          LOG.error("Can't send heartbeat to CloudataMaster: " + hostName + ": " + e.getMessage());
        }
        try {
          sleep(1000);
        } catch (InterruptedException e) {
          return;
        }
      }
    }
  }
  
  class Listener extends Thread implements ListenerTestIF {
    final Log LOG = LogFactory.getLog(Listener.class);

    private boolean shutdown = false;
    final Selector acceptorSelector;
    final ServerSocketChannel ssc;

    final WorkerPool workerPool;

    final int pipeNo = 0;
    final int port;
    final int pipeTimeout;
    final int numWorkers;
    final String logPath;
    ListenerTestIF testProxy;

    Listener(CloudataConf conf, String logPath) throws IOException {
      this.port = conf.getInt("cloudata.commitlog.server.port", 57001) + CommitLogServer.PORT_DIFF;
      this.pipeTimeout = conf.getInt("cloudata.commitlog.server.pipe.timeout", 10);
      this.logPath = logPath;
      this.numWorkers = conf.getInt("commitLogServer.worker.count", numProcessors);
      workerPool = new WorkerPool(numWorkers, port);

      acceptorSelector = Selector.open();
      ssc = ServerSocketChannel.open();
      ssc.socket().bind(new InetSocketAddress(port));
      ssc.socket().setReuseAddress(true);
      ssc.configureBlocking(false);
      LOG.info("# of Workers : " + numWorkers + ", Pipe port : " + port + " & timeout : " + pipeTimeout + " s, log path : "
          + logPath);

      testProxy = this;
      if (CommitLogServer.IS_TEST_MODE) {
        testProxy = FaultInjectionProxy.wrap(this, ListenerTestIF.class);
      }
    }

    public int getPipePort() {
      return port;
    }

    public void stopListening() {
      LOG.info("stop listening");
      shutdown = true;
      acceptorSelector.wakeup();
      workerPool.shutdown();
    }

    public void run() {
      this.setName("listener thread [" + port + "]");

      try {
        ssc.register(acceptorSelector, SelectionKey.OP_ACCEPT);
      } catch (ClosedChannelException e) {
        e.printStackTrace();
        return;
      }

      LOG.info("Listening thread starts");

      while (!shutdown) {
        int numReady = 0;

        try {
          numReady = acceptorSelector.select();

          if (numReady > 0) {
            dispatchSelectedKeys();
          }
        } catch (IOException e) {
          LOG.warn("Exception in accepting new pipe", e);
          break;
        }
      }

      try {
        ssc.close();
        acceptorSelector.close();
      } catch (IOException e) {
        LOG.warn("closing server socket channel is fail", e);
      }

      LOG.info("Listening thread is done");
    }

    private void dispatchSelectedKeys() {
      Iterator<SelectionKey> iter = acceptorSelector.selectedKeys().iterator();

      while (iter.hasNext()) {
        SelectionKey key = iter.next();
        iter.remove();

        if (key.isValid()) {
          if (key.isAcceptable()) {
            testProxy.handleNewConnection(key);
          }
        }
      }
    }

    public void handleNewConnection(SelectionKey key) {
      SocketChannel channel = null;

      try {
        channel = ssc.accept();
      } catch (IOException e) {
        LOG.warn("error in accepting new connection due to", e);
        return;
      }

      Pipe pipe = null;

      try {
        pipe = new Pipe(channel, pipeTimeout
            , logPath, Constants.PIPE_CL_FILE_NAME + listener.port);
        
        pipe.setEventHandler(makePipeEventHandler());
        pipe.setListenPortNumber(port);

        workerPool.getWorker().append(pipe);
      } catch (CommitLogShutDownException e) {
        stopCommitLogServer();
      } catch (IOException e) {
        LOG.warn("error in creating pipe due to ", e);

        try {
          channel.close();
        } catch (IOException e1) {
          LOG.warn("closing channel is fail", e1);
        }

        return;
      }
    }

    private PipeEventHandler makePipeEventHandler() {
      return new PipeEventHandler() {
        public void pipeEstablished(String pipeKey, Pipe pipe)
            throws IOException {
          synchronized (establishedPipeMap) {
            establishedPipeMap.put(pipeKey, pipe);
          }
          LOG.debug("Pipe key[" + pipeKey + "] is registered");
        }

        public void pipeClosed(String pipeKey) {
          synchronized (establishedPipeMap) {
            if (establishedPipeMap.remove(pipeKey) != null) {
              LOG.debug("Pipe key[" + pipeKey + "] is removed");
            }
          }
        }
      };
    }
  }

  // // RPC METHODS ////

  public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
    return 1l;
  }

  double calc(long after, long before) {
    return (after - before) / 1000000.0;
  }

  public String rollback(String dirName, String position) {
    LOG.debug("rollback is called! with dirName : " + dirName);
    RandomAccessFile file = null;
    try {
      File logFile = new File(commitLogStoreFile, 
          CommitLogFileChannel.getLogFileName(dirName, Constants.PIPE_CL_FILE_NAME + listener.port));

      file = new RandomAccessFile(logFile, "rws");
      file.getChannel().truncate(Long.parseLong(position));
    } catch (IOException e) {
      LOG.warn("rollbacking file is fail dir [" + dirName + "], pos : " + position, e);
      return "ABORT";
    } finally {
      if (file != null) {
        try {
          file.close();
        } catch (IOException e) {
        }
      }
    }

    return "OK";
  }
  
  public String mark(String dirName) {
    try {
      long pos = CommitLogFileChannel.mark(dirName, commitLogStoreFile.getAbsolutePath()
          , Constants.PIPE_CL_FILE_NAME + listener.port);
	  LOG.info("mark dir[" + dirName + "] pos : " + pos);
    } catch(IOException e) {
      LOG.warn("fail to backupLogFile of dir [" + dirName + "]");
      return "NOT OK";
    }
    
    return "OK";
  }
  
  public void restoreMark(String dirName) {
    try {
      CommitLogFileChannel.cancelLastMark(dirName, commitLogStoreFile.getAbsolutePath()
          , Constants.PIPE_CL_FILE_NAME + listener.port);
    } catch(IOException e) {
      LOG.warn("fail to backupLogFile of dir [" + dirName + "]");
    }
  }

  public String backupLogFile(String dirName) {
    LOG.debug("backupLogFile enters dir[" + dirName + "]");
    try {
      CommitLogFileChannel.backup(dirName, commitLogStoreFile.getAbsolutePath()
    		  , Constants.PIPE_CL_FILE_NAME + listener.port);
    } catch(IOException e) {
      LOG.warn("fail to backupLogFile of dir [" + dirName + "]");
      return "NOT OK";
    } 

    LOG.debug("backupLogFile exits dir[" + dirName + "]");

    return "OK";
  }

  public void restoreLogFile(String dirName) {
    CommitLogFileChannel channel = null;
    try {
      channel = CommitLogFileChannel.openChannel(dirName, 
    		  commitLogStoreFile.getAbsolutePath(), Constants.PIPE_CL_FILE_NAME + listener.port);
      channel.restoreLastLog();
    } catch(IOException e) {
      LOG.warn("fail to restoreLogFile of dir [" + dirName + "]");
    } finally {
      if (channel != null) {
        try {
          CommitLogFileChannel.closeChannel(channel);
        } catch (IOException e) {
          LOG.warn("fail to close commit log file channel due to " + e);
        }
      }
    }

  }

  // by sangchul : 같은 VM에서 실행되는 CommitLogServer들의 동기화를 위해
  static final ReentrantLock formatLock = new ReentrantLock();

  public void format() {
    formatLock.lock();

    final File tempFile = new File(makeTempLogStorePath());

    try {
      if (commitLogStoreFile.exists() == false) {
        return;
      }

      if (commitLogStoreFile.renameTo(tempFile) == false) {
        LOG.warn("Fail to format. Renaming commitlog directory[" 
        		+ commitLogStoreFile.getAbsolutePath() + "] to [" 
        		+ tempFile.getAbsolutePath() + "]");
        return;
      }
      
      if (commitLogStoreFile.mkdirs() == false) {
        LOG.warn("Fail to make commit log directory[" + commitLogStoreFile.getAbsolutePath() + "]");
      }
    } finally {
      formatLock.unlock();
    }

    createCleanThread(tempFile).start();
    LOG.info("Starting file format thread");
  }

  private Thread createCleanThread(final File tempFile) {
    return new Thread() {
      public void run() {
        clean(tempFile);
        LOG.info("Finishing file format thread");
      }

      private void clean(File file) {
        File[] fileList = null;
        if (file.isDirectory() && (fileList = file.listFiles()) != null) {
          for (File childrenFile : fileList) {
            clean(childrenFile);
          }
        }

        if (file.delete() == false) {
          LOG.warn("Deleting " + file.getAbsolutePath() + " is fail");
        }
      }
    };
  }

  private String makeTempLogStorePath() {
    String result;
    String commitLogStorePath = commitLogStoreFile.getAbsolutePath();
    
    if (commitLogStorePath.charAt(commitLogStorePath.length() - 1) == '\\'
        || commitLogStorePath.charAt(commitLogStorePath.length() - 1) == '/') {
      result = commitLogStorePath.substring(0, commitLogStorePath.length() - 1);
    } else {
      result = commitLogStorePath;
    }

    return result + "_" + System.currentTimeMillis();
  }

  public String removeAllLogs(final String dirName) throws IOException {
    LOG.debug("removeAllLogs is called! with dirName : " + dirName);

    if (commitLogStoreFile.exists()) {
      Thread removeAllLogsThread = new Thread() {
        public void run() {
          try {
            File[] files = commitLogStoreFile.listFiles(new FileFilter() {
				public boolean accept(File pathname) {
					return pathname.getName().indexOf(dirName) >= 0;
				}
            });
            
            for(File file : files) {
            	if (!file.delete()) {
                    LOG.warn("Cannot delete log file [" + file.getAbsolutePath() + "]");
            	}
            }
          } catch(Exception e) {
            LOG.error("fail to remove all logs dir[" + dirName + "]", e);
          } finally {
            LOG.debug("removeAll dirName[" + dirName + "] exits");
          }
        }
      };
 
      removeAllLogsThread.start();
    }

    return "OK";
  }

  public void removeBackupLogs(String dirName) throws IOException {
    CommitLogFileChannel channel = null;
    try {
      channel = CommitLogFileChannel.openChannel(dirName, commitLogStoreFile.getAbsolutePath(), 
          Constants.PIPE_CL_FILE_NAME + listener.port);
      channel.removeBackupFiles();
    } catch(IOException e) {
      LOG.warn("fail to removeBackupFiles of dir [" + dirName + "]");
    } finally {
      if (channel != null) {
        try {
          CommitLogFileChannel.closeChannel(channel);
        } catch (IOException e) {
          LOG.warn("fail to close commit log file channel due to " + e);
        }
      }
    }
  }
  
  File getLogIndexFile(String dirName) {
    return new File(commitLogStoreFile
        , CommitLogFileChannel.getIndexFileName(dirName
                , Constants.PIPE_CL_FILE_NAME + listener.port));  
  }

  File getLogFile(String dirName) {
    return new File(commitLogStoreFile
    		, CommitLogFileChannel.getLogFileName(dirName
    				, Constants.PIPE_CL_FILE_NAME + listener.port));
  }

  public long getLogFileSize(String dirName) {
    LOG.debug("getLogFileSize is called! with dirName : " + dirName);

    File log = getLogFile(dirName);
    if (log.exists() == false) {
      LOG.warn("getting file size of dir[" + dirName + "] is fail. file does not exist");
    }

    return log.length();
  }


  public void stopCommitLogServer() {
    try {
      this.stop();
    } catch (java.lang.reflect.UndeclaredThrowableException e) {
    }

    LOG.info("Commit log server is stopped");
    System.exit(-1);
  }

  public int readAllLogs(String dirName) {
    LOG.debug("readAllLogs is called with dirName : " + dirName);
    File[] fileList = commitLogStoreFile.listFiles();
    if (commitLogStoreFile.exists() == false || fileList.length == 0) {
      LOG.warn("A dir directory does not exist");
      return -1;
    }
    
    ArrayList<FileChannel> chList = new ArrayList<FileChannel>();
    for(int i = 0; i < fileList.length; i++) {
      if (fileList[i].getName().startsWith(dirName))
      try {
        chList.add(new FileInputStream(fileList[i]).getChannel());
      } catch (FileNotFoundException e) {
        LOG.warn("File [" + fileList[i].getName() + "] does not exist in reading logs");
      }
    }

    return startFileTransferChannel(dirName, chList.toArray(new FileChannel[0]));
  }
  
  public int readLastLogFromMarkedPosition(String dirName) {
    File logFile = getLogFile(dirName);
    File indexFile = getLogIndexFile(dirName);

    FileChannel ch = null;
    try {
      long index = 0;

      if (logFile.exists() == false) {
        LOG.warn("A directory [" + dirName + "] or log file does not exist");
        return -1;
      } else if (indexFile.exists()) {
        FileChannel indexChannel = new FileInputStream(indexFile).getChannel();
        index = CommitLogFileChannel.readLastIndex(indexChannel);
        LOG.info("index of dir [" + dirName + "] : " + index);
        indexChannel.close();
      }
      
      FileInputStream in = new FileInputStream(logFile);
      ch = in.getChannel();
      ch.position(index);
    } catch(IOException e) {
      LOG.error("Fail to read logs", e);
      return -1;
    }

    return startFileTransferChannel(dirName, ch);
  }

  public int readLastLog(String dirName) {
    //LOG.debug("readLastLog is called with dirName : " + dirName);

    if (commitLogStoreFile.exists() == false) {
      LOG.warn("A dir directory does not exist");
      return -1;
    }

    File[] fileList = commitLogStoreFile.listFiles();
    if (fileList == null || fileList.length == 0) {
      LOG.warn("file does not exist");
      return -1;
    }

    File lastFile = null;
    long lastModified = 0l;

    for (int i = 0; i < fileList.length; i++) {
      if (fileList[i].getName().startsWith(dirName) && fileList[i].lastModified() > lastModified) {
        lastFile = fileList[i];
        lastModified = lastFile.lastModified();
      }
    }

    if (lastFile.length() == 0) {
      LOG.warn("the length of the file is zero");
      return -1;
    }

    try {
      FileChannel ch = new FileInputStream(lastFile).getChannel();
      ch.position(0);
      return startFileTransferChannel(dirName, ch);
    } catch (FileNotFoundException e) {
      LOG.warn("File : " + lastFile.getName() + " does not exist", e);
    } catch (IOException e) {
      LOG.warn("Fail to set position to zero", e);
    }
    return -1;
  }

  public String dirExists(String dirName) {
    //LOG.debug("dirExists is called with dirName : " + dirName);

    return getLogFile(dirName).exists() ? "true" : "false";
  }
  
  private int startFileTransferChannel(String dirName, FileChannel ch) {
    return startFileTransferChannel(dirName, new FileChannel[] { ch });
  }

  private int startFileTransferChannel(String dirName, FileChannel[] fileChannelList) {
    int port = -1;
    try {
      ServerSocketChannel ssc = ServerSocketChannel.open();
      ssc.configureBlocking(false);
      ServerSocket serverSocket = ssc.socket();
      serverSocket.bind(null);
      port = serverSocket.getLocalPort();

      new FileTransferThread(dirName, ssc, fileChannelList).start();
      //LOG.info("File transfer thread is started and read method is done");
    } catch (IOException e) {
      LOG.warn("starting file transfer is fail", e);
    }

    return port;
  }

  public long getChecksum(String dirName) throws IOException {
    File logFile = getLogFile(dirName);

    long fileLength = logFile.length();

    CheckedInputStream cksumIn = new CheckedInputStream(new FileInputStream(logFile), new CRC32());
    BufferedInputStream in = new BufferedInputStream(cksumIn, 8192);

    for (long i = 0; i < fileLength; i++) {
      in.read();
    }

    return cksumIn.getChecksum().getValue();
  }

  public void test() {
  }
  
  public DiskInfo getDiskInfo() throws IOException {
    return df.getDiskInfo();
  }
  
  public ServerMonitorInfo getServerMonitorInfo() throws IOException {
    ServerMonitorInfo serverMonitorInfo = new ServerMonitorInfo();
    
    serverMonitorInfo.setDiskInfo(getDiskInfo());
    serverMonitorInfo.setLogDirUsed(du.getUsed());
    
    List<String> logFiles = new ArrayList<String>();
    File[] files = commitLogStoreFile.listFiles();

    if(files != null) {
      String prefix = Constants.PIPE_CL_FILE_NAME + listener.port;
      for(File eachFile: files) {
        if(eachFile.getName().indexOf(prefix) >= 0 && 
            eachFile.getName().indexOf("index") < 0) {
          logFiles.add(eachFile.getName() + "(" + eachFile.length()/1024 + " KB)");
        }
      }
    }
    serverMonitorInfo.setLogFiles(logFiles);
    serverMonitorInfo.setLogPath(commitLogStoreFile.getPath());
    
    serverMonitorInfo.setServerStartTime(serverStartTime);
    serverMonitorInfo.setFreeMemory(Runtime.getRuntime().freeMemory());
    serverMonitorInfo.setMaxMemory(Runtime.getRuntime().maxMemory());
    serverMonitorInfo.setTotalMemory(Runtime.getRuntime().totalMemory());

    
    return serverMonitorInfo;
  }

}
