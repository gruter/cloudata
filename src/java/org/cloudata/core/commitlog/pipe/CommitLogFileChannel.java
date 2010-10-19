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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.CStopWatch;


/**
 * @author sangchul 하나의 CommitLogFileChannel 인스턴스는 Tablet하나를 가리킨다. 따라서 만일 하나의
 *         tablet에 동시에 변경 요청이 수신 될 경우 두개 이상의 Pipe가 동시에 하나의 CommitLogFileChannel
 *         인스턴스에 접근할 수 있다. 따라서 적절한 동기화가 필요하다. 이를테면 쓰기 작업 시 seq 번호를 부여해서 쓰기 순서도
 *         맞춰야 한다.
 */
public class CommitLogFileChannel {
  // STATIC FUNCTIONS & VARIABLES
  
  private static final Log LOG = LogFactory.getLog(CommitLogFileChannel.class);

  private static ReentrantReadWriteLock mapLock = new ReentrantReadWriteLock();
  private static HashMap<String, CommitLogFileChannel> channelMap 
    = new HashMap<String, CommitLogFileChannel>();
  private static FileChannelCullerThread fileChannelCuller;
  
  static {
    fileChannelCuller = new FileChannelCullerThread();
    fileChannelCuller.setDaemon(true);
    fileChannelCuller.setName("FileChannelCuller");
    fileChannelCuller.start();
  }

  public static CommitLogFileChannel openChannel(String dirName, String path,
      String fileName) throws IOException {
    CommitLogFileChannel ch = null;
    String key = getLogFileName(dirName, fileName);
    
    mapLock.readLock().lock();
    try {
      if ((ch = channelMap.get(key)) != null) {
        ch.numPipes.incrementAndGet();
        return ch;
      }
    } finally {
      mapLock.readLock().unlock();
    }

    mapLock.writeLock().lock();
    try {
      if ((ch = channelMap.get(key)) == null) {
        LOG.debug("create file channel. dirName[" + dirName + "], fileName[" + fileName + "]");
        ch = new CommitLogFileChannel(dirName, path, fileName);
        channelMap.put(key, ch);
      }
    } finally {
      ch.numPipes.incrementAndGet();
      mapLock.writeLock().unlock();
    }

    // by sangchul
    // all the threads which tried to make new instance should call this method
    // because of waiting for instance initialization.
    ch.init();
    
    return ch;
  }

  public static void closeChannel(CommitLogFileChannel ch) throws IOException {
    mapLock.readLock().lock();
    try {
      if (ch.numPipes.decrementAndGet() > 0) {
        return;
      }
    } finally {
      mapLock.readLock().unlock();
    }
      
    String key = ch.logFileName;
    boolean close = false;

    mapLock.writeLock().lock();
    try {
      if (ch.numPipes.get() <= 0) {
        LOG.debug("close file channel. key[" + key + "]");
        channelMap.remove(key);
        close = true;
      }
    } finally {
      mapLock.writeLock().unlock();
      if (close) {
        ch.internalClose();
      }
    }
  }

  public static void backup(String dirName, String path, String fileName)
      throws IOException {
    CommitLogFileChannel ch = openChannel(dirName, path, fileName);
    try {
      ch.backup();
    } finally {
      closeChannel(ch);
    }
  }
  
  public static long mark(String dirName, String path, String fileName) throws IOException {
    CommitLogFileChannel ch = openChannel(dirName, path, fileName);
    try {
      return ch.mark();
    } finally {
      closeChannel(ch);
    }
  }
  
  public static void cancelLastMark(String dirName, String path, String fileName) throws IOException {
    CommitLogFileChannel ch = openChannel(dirName, path, fileName);
    try {
      ch.cancelLastMark();
    } finally {
      closeChannel(ch);
    }
  }
  
  public static long readLastIndex(FileChannel ch) throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(Long.SIZE / 8);
    long chSize = ch.size();
    
    if (chSize > 0) {
      ch.position(chSize > (Long.SIZE / 8) ? chSize - (Long.SIZE / 8) : 0);
      ch.read(buf);
      buf.flip();
      return buf.getLong();
    } else {
      return 0;
    }
  }
  
  public static String getLogFileName(String dirName, String fileName) {
    return dirName + "_" + fileName;
  }
  
  public static String getIndexFileName(String dirName, String fileName) {
    return dirName + "_" + Constants.PIPE_IDX_FILE_NAME + "_" + fileName;
  }
  // MEMBER FUNCTIONS & VARIABLES
  
  FileChannel channel;
  FileChannel indexChannel;

  long rollbackIndex;
  final String indexFileFullPath;
  final String logFileFullPath;
  final String logFileName;
  final String dirName;
  final ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
  final File parentDir;
  AtomicInteger numPipes = new AtomicInteger(0);
  ByteBuffer indexBuf = ByteBuffer.allocate(Long.SIZE / 8);

  private CommitLogFileChannel(String dirName, String path, String fileName) throws IOException {
    this.dirName = dirName;
    this.parentDir = new File(path);

    this.logFileName = getLogFileName(dirName, fileName);
    this.logFileFullPath = parentDir.getAbsolutePath() + File.separator + logFileName;
    this.indexFileFullPath = parentDir.getAbsolutePath() + File.separator 
      + getIndexFileName(dirName, fileName);
  }

  synchronized void init() throws IOException {
    if (channel != null) {
      return;
    }
    
    FileOutputStream fos = null;

    //NStopWatch watch = new NStopWatch();
    try {
      fos = new FileOutputStream(logFileFullPath, true);
      //watch.stopAndReportIfExceed(LOG);
    } catch (FileNotFoundException e) {
      if (parentDir.exists() == false && parentDir.mkdirs() == false && parentDir.exists() == false) {
        throw new IOException("Making directory to " + parentDir.getAbsolutePath() + " is fail");
      }

      fos = new FileOutputStream(logFileFullPath, true);
      //watch.stopAndReportIfExceed(LOG);
    }

    channel = fos.getChannel();
    
    fos = new FileOutputStream(indexFileFullPath, true);
    //watch.stopAndReportIfExceed(LOG);

    indexChannel = fos.getChannel();
    
    numPipes.incrementAndGet();
    fileChannelCuller.append(this);
  }

  public synchronized boolean isOpen() {
    if (channel == null) {
      return false;
    }

    return channel.isOpen();
  }

  ByteBuffer headerBuf = ByteBuffer.allocate(4);

  private long lastActivity;

  public synchronized void write(ByteBuffer[] bufferArray) throws IOException {
    // by sangchul
    // check whether this CommitLogFileChannel is initialized or not. 
    // If not, this thread should initialize the instance and then continue writing.
    // Or, it is possible for other thread to initialize the instance, and then 
    // this thread should wait for the thread finishing initialization.
    init();

    //NStopWatch watch = new NStopWatch();
    //watch.start("actualWriting", 1000);
    try {
      headerBuf.putInt(getTotalLengthOf(bufferArray));
      headerBuf.flip();
      channel.write(headerBuf);
      headerBuf.clear();

      channel.write(bufferArray);
    } catch(IOException e) {
      LOG.warn("fail to write log. full path [" + logFileFullPath + "]", e);
      internalClose();
      throw e;
    } finally {
      //watch.stopAndReportIfExceed(LOG);
      lastActivity = System.currentTimeMillis();
    }
  }
  
  private int getTotalLengthOf(ByteBuffer[] bufferArray) {
    int totalLength = 0;

    for (ByteBuffer buf : bufferArray) {
      totalLength += buf.limit();
    }

    return totalLength;
  }

  private void internalClose() throws IOException {
    if (isOpen()) {
      channel.close();
      channel = null;
      indexChannel.close();
      indexChannel = null;
    }
  }

  public synchronized void removeFile() {
    try {
      internalClose();
    } catch (IOException e) {
      LOG.warn("closing file before removing is fail", e);
    }

    File file = new File(logFileFullPath);

    if (file.exists()) {
      if (file.delete()) {
        LOG.info("commit log file [" + file.getPath() + "] is deleted");
      } else {
        LOG.warn("tried to delete commit log file but it is NOT deleted.");
      }
    } else {
      LOG.info("attemped to remove file [" + file.getPath() + "], but not exists");
    }
  }

  public synchronized long getFileSize() throws IOException {
    File file = new File(logFileFullPath);
    return file.length();
  }
  
  private synchronized long mark() throws IOException {
    if (!isOpen()) {
      return -1;
    }
    
    long size = channel.size();
    indexBuf.clear();
    indexBuf.putLong(size);
    indexBuf.flip();
    indexChannel.write(indexBuf);
    indexBuf.clear();
	return size;
  }
  
  private synchronized void cancelLastMark() throws IOException {
    if (!isOpen()) {
      return;
    }
    
    try {
      channel.truncate(readLastIndex(indexChannel));
    } catch(Exception e) {
      LOG.warn("Fail to truncate channel", e);
    }
    
    long pos = indexChannel.size() - Long.SIZE / 8;
    if (pos < 0) {
      pos = 0;
    }
    
    indexChannel.truncate(pos);
  }
  
  public synchronized void backup() throws IOException {
    long beforeClose = System.currentTimeMillis();
    try {
      internalClose();
    } catch (IOException e) {
      LOG.warn("closing file before backing up is fail", e);
    }

    long beforeRename = System.currentTimeMillis();
    File file = new File(logFileFullPath);
    if (!file.renameTo(new File(parentDir, file.getName() + "_" + System.currentTimeMillis()))) {
      throw new IOException("fail to backup [" + file.getAbsolutePath() + "]");
    }

    long beforeInit = System.currentTimeMillis();
    init();

    long finish = System.currentTimeMillis();

    if ((finish - beforeInit) >= 100) {
      LOG.info("too long time to init : " + (finish - beforeInit) + "ms");
    } else if ((beforeInit - beforeRename) >= 100) {
      LOG.info("too long time to rename : " + (beforeInit - beforeRename) + "ms");
    } else if ((beforeRename - beforeClose) >= 100) {
      LOG.info("too long time to close : " + (beforeRename - beforeClose) + "ms");
    }
  }

  public synchronized void restoreLastLog() throws IOException {
    try {
      internalClose();
    } catch (IOException e) {
      LOG.warn("closing file in restoring file is fail", e);
    }

    File lastLogFile = null;

    long lastModified = 0l;
    if (parentDir.listFiles() != null) {
      for (File file : parentDir.listFiles()) {
        if (lastLogFile == null) {
          lastLogFile = file;
          continue;
        }

        if (lastModified < file.lastModified()
            && lastLogFile.getName().equals(logFileName) == false) {
          lastModified = file.lastModified();
          lastLogFile = file;
        }
      }

      lastLogFile.renameTo(new File(parentDir, logFileName));
    }

    init();
  }

  public synchronized void removeBackupFiles() {
    File[] fileList = parentDir.listFiles(new FilenameFilter() {
      public boolean accept(File dir, String name) {
        return name.equals(logFileName) == false;
      }
    });

    if (fileList != null) {
      for (File file : fileList) {
        LOG.info("remove backup file [" + file.getAbsolutePath() + "]");
        if (!file.delete()) {
          LOG.warn("fail to remove backup file [" + file.getAbsolutePath() + "]");
        }
      }
    }
  }

  public synchronized boolean isExpired() {
    return System.currentTimeMillis() - lastActivity > 30000;
  }
}

class FileChannelCullerThread extends Thread {
  private static final Log LOG = LogFactory.getLog(FileChannelCullerThread.class);
  LinkedList<CommitLogFileChannel> channelList = new LinkedList<CommitLogFileChannel>();
  
  public void run() {
    while(true) {
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        LOG.info("FileChannelCullerThread is done");
        break;
      }
      
      ArrayList<CommitLogFileChannel> expiredList = new ArrayList<CommitLogFileChannel>();
      
      synchronized(channelList) {
        Iterator<CommitLogFileChannel> iter = channelList.iterator();
        while(iter.hasNext()) {
          CommitLogFileChannel ch = iter.next();
          if (ch.isExpired()) {
            expiredList.add(ch);
            iter.remove();
          }
        }
      }
      
      for(CommitLogFileChannel ch : expiredList) {
        try {
          CommitLogFileChannel.closeChannel(ch);
        } catch (IOException e) {
          LOG.warn("exception in file channel culler thread", e);
        }
      }
    }
  }
  
  public void append(CommitLogFileChannel ch) {
    synchronized(channelList) {
      channelList.addLast(ch);
    }
  }
}
