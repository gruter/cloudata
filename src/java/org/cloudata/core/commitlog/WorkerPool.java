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
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.commitlog.pipe.AsyncFileWriter;
import org.cloudata.core.commitlog.pipe.Pipe;
import org.cloudata.core.common.CStopWatch;


public class WorkerPool {
  static long lastReportTime = 0l;
  static final Log LOG = LogFactory.getLog(WorkerPool.class);
  List<Worker> workerList = new ArrayList<Worker>();
  int port;
  AtomicBoolean shutdown = new AtomicBoolean(false);
  AsyncFileWriter asyncWriter; 

  WorkerPool(int size, int port) throws IOException {
    this.port = port;
    this.asyncWriter = new AsyncFileWriter();
    this.asyncWriter.start();

    for (int i = 0; i < size; i++) {
      Worker worker = new Worker(this, i + 1, port, asyncWriter);
      workerList.add(worker);
      worker.start();
    }
  }

  public Worker getWorker() throws IOException {
    if (shutdown.get()) {
      throw new CommitLogShutDownException(new IOException("Worker is shutdown"));
    }
    
    int minimum = Integer.MAX_VALUE;
    Worker idleWorker = null;

    for (Worker worker : workerList) {
      int pipeCount = worker.getPipeCount();
      if (pipeCount == 0) {
        return worker;
      } else if (minimum > pipeCount) {
        minimum = pipeCount;
        idleWorker = worker;
      }
    }

    return idleWorker;
  }

  void report() {
    for (Worker worker : workerList) {
      LOG.info("worker[" + worker.getName() + "] pipe count : " + worker.pipeSet.size());
    }
  }

  public void shutdown() {
    if (!shutdown.get()) {
      for (Worker worker : workerList) {
        worker.shutdown();
      }
      
      shutdown.set(true);      
    }
  }
}

class Worker extends Thread {
  static final Log LOG = LogFactory.getLog(Worker.class);

  AtomicBoolean doSelectingBlock = new AtomicBoolean(true);
  final Selector selector;
  final AsyncFileWriter asyncWriter;
  LinkedBlockingQueue <Pipe> newPipeList = new LinkedBlockingQueue <Pipe>();
  ConcurrentSkipListSet<Pipe> pipeSet = new ConcurrentSkipListSet<Pipe>();
  
  final int workerNo;
  final int port;

  boolean shutdown = false;

  final WorkerPool workerPool;

  Worker(WorkerPool workerPool, int workerNo, int port, AsyncFileWriter asyncWriter) throws IOException {
    this.workerNo = workerNo;
    this.port = port;
    this.workerPool = workerPool;
    this.asyncWriter = asyncWriter;
    
    this.setName("Worker #" + workerNo + " on " + port);
    selector = Selector.open();
  }

  public void shutdown() {
    asyncWriter.stop();
    shutdown = true;
    LOG.debug(this.getName() + " is interrupted. Current num of pipes are " + pipeSet.size());
    this.interrupt();
    selector.wakeup();
  }

  public int getPipeCount() {
    return pipeSet.size();
  }

  long startTime = 0;
  long processTime = 0;
  long cleanupTime = 0;
  
  public void run() {
    try {
      int numSelected = 0;
      while (!shutdown) {
        checkNewPipeEntry();
        checkNumOfPipes(false);
        
        long endTime = System.currentTimeMillis();
        
        if (cleanupTime > 0 && (cleanupTime - endTime) > 100) {
          LOG.info("TIME REPORT [too long time to check new pipe] takes " + (cleanupTime - endTime) + " ms");
        }

        try {
          if (doSelectingBlock.get()) {
            numSelected = selector.select(100);
          } else {
            numSelected = selector.selectNow();
          }
        } catch (IOException e) {
          LOG.warn("Exception in selecting events", e);
        }

        startTime = System.currentTimeMillis();
        
        if (numSelected > 0) {
          dispatchSelectedKeys();
        }

        processTime = System.currentTimeMillis();
        
        if (processTime - startTime > 100) {
          LOG.info("TIME REPORT [too long time to process events] takes " 
              + (processTime - startTime));
        }

        cleanupExpiredPipes(processTime - startTime);
        
        cleanupTime = System.currentTimeMillis();
        
        if (cleanupTime - processTime >= 3) {
          LOG.info("TIME REPORT [too long time to cleanup expired pipes] takes " 
              + (cleanupTime - processTime));
        }
      }

      cleanupAll();
      LOG.debug(this.getName() + " is done");
    } catch (Throwable e) {
      LOG.warn("Worker [" + getName() + "] exits unexpectedly due to ", e);
      cleanupAll();
      System.gc();
      LOG.info("FREE : " + Runtime.getRuntime().freeMemory() + " Bytes, TOTAL : " + Runtime.getRuntime().totalMemory() + "Bytes");
      asyncWriter.stop();

      try {
        LOG.info("spawn new worker #" + workerNo + " on port " + this.port);
        Worker worker = new Worker(workerPool, this.workerNo, this.port, this.asyncWriter);
        worker.setName(getName());
        workerPool.workerList.add(worker);
        worker.start();
      } catch (IOException ex) {
        LOG.warn("fail to fork new worker thread", ex);
      }
    }
  }

  private void dispatchSelectedKeys() {
    Iterator<SelectionKey> iter = selector.selectedKeys().iterator();

    while (iter.hasNext()) {
      SelectionKey key = iter.next();
      iter.remove();

      if (key.isValid()) {
        if (key.isReadable()) {
          handleData(key);
        } else if (key.isConnectable()) {
          handleConnection(key);
        } else if (key.isWritable()) {
          handleWrite(key);
        }
      }
    }
  }

  private void handleWrite(SelectionKey key) {
    Pipe pipe = (Pipe) key.attachment();

    try {
      pipe.write(key.channel());
    } catch (CommitLogShutDownException e) {
      workerPool.shutdown();
    } catch (Exception e) {
      LOG.error("error in handling write due to : ", e);
      cleanupErrorPipe(pipe);
    }
  }

  private void handleConnection(SelectionKey key) {
    Pipe pipe = (Pipe) key.attachment();

    try {
      pipe.connect(key.channel());
    } catch (CommitLogShutDownException e) {
      workerPool.shutdown();
    } catch (Exception e) {
      LOG.error("error in handling connection due to : ", e);
      cleanupErrorPipe(pipe);
    }
  }

  private void handleData(SelectionKey key) {
    Pipe pipe = (Pipe) key.attachment();

    try {
      pipe.read(key.channel());
    } catch (CommitLogShutDownException e) {
      workerPool.shutdown();
    } catch (Exception e) {
      if (e instanceof PipeNormallyClosed) {
        pipeSet.remove(pipe);
        pipe.close();
        LOG.debug("Pipe#" + pipe.getPipeKey() + " is gracefully closed.");
      } else {
        LOG.error("error in reading data due to ", e);
        cleanupErrorPipe(pipe);
      }
    }
  }

  // // cleanup class and methods ////

  private void cleanupExpiredPipes(long processTime) {
    Iterator<Pipe> iter = pipeSet.iterator();
    while (iter.hasNext()) {
      Pipe pipe = iter.next();

      if (pipe.isExpired(processTime)) {
        LOG.warn(pipe + " is closed since it is expired.");
        iter.remove();
        pipe.close();
      }
    }
  }

  private void cleanupAll() {
    LOG.debug("CLEAN UP ALL THE PIPES");
    for (Pipe pipe : pipeSet) {
      pipe.close();
    }

    pipeSet.clear();

    try {
      selector.close();
    } catch (IOException e) {
      LOG.warn("closing selector is fail", e);
    }
  }

  private void cleanupErrorPipe(Pipe pipe) {
    LOG.info("clean up error pipe : " + pipe);
    pipeSet.remove(pipe);
    pipe.close();
  }

  private void checkNewPipeEntry() {
    if (pipeSet.isEmpty()) {
      synchronized (newPipeList) {
        while (newPipeList.isEmpty()) {
          try {
            LOG.debug("Waiting for new pipe");
            newPipeList.wait();
          } catch (InterruptedException e) {
            return;
          }
        }
        registerNewPipe();
      }
    } else {
      // Do not need to lock the monitor of newPipeList
      if (newPipeList.isEmpty() == false) {
        CStopWatch watch = new CStopWatch();
        watch.start("registerNewPipe", 10);
        registerNewPipe();
        watch.stopAndReportIfExceed(LOG);
      }

      doSelectingBlock.set(true);
    }
  }
  
  private void registerNewPipe() {
    while(true) {
      Pipe pipe = newPipeList.poll();
      if (pipe == null) {
        break;
      }
      
      LOG.debug(Thread.currentThread().getName()
          + " starts processing");

      try {
        pipe.register(selector, asyncWriter);
        pipe.init();
      } catch (IOException e) {
        LOG.warn("initalizing pipe is fail", e);
        return;
      }

      if (pipeSet.add(pipe) == false) {
        LOG.error("Error in adding pipe to pipeSet since the set has already same pipe!");
        pipe.close();
      }
    }
  }

  static volatile long lastCheckedTime;

  private void checkNumOfPipes(boolean report) {
    long curTime = System.currentTimeMillis();

    if (report || (curTime - lastCheckedTime) > 60000) { // 1 MIN
      LOG.debug("num of pipes in this worker : " + pipeSet.size());
      lastCheckedTime = curTime;
    }
  }

  public void append(Pipe pipe) {
    synchronized(newPipeList) {
      newPipeList.add(pipe);
      newPipeList.notify();
    }
    selector.wakeup();
    doSelectingBlock.set(false);
  }
}
