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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.Pipe.SinkChannel;
import java.nio.channels.Pipe.SourceChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.commitlog.PipeNormallyClosed;


public class Pipe implements Comparable<Pipe> {
  static final Log LOG = LogFactory.getLog(Pipe.class);
  public static int G_PORT = 18000;
  static final AtomicLong pipeCount = new AtomicLong(0);

  Object stateChangeLock = new Object();
  PipeState state;

  String pipeKey;
  Context ctx;
  volatile long lastTouchTime = System.currentTimeMillis();
  PipeEventHandler eventHandler;
  int pipeLifeTime;
  AtomicBoolean activated = new AtomicBoolean();
  final long pipeNo = pipeCount.getAndIncrement();

  public Pipe(SocketChannel prevChannel, int timeout, String logPath, String fileName) throws IOException {
    this.pipeLifeTime = timeout * 1000;
    this.ctx = new Context(prevChannel, logPath, fileName);
    this.state = InitState.instance();
    activated.set(true);
  }
  
  public boolean equals(Object obj) {
    if (obj instanceof Pipe == false) {
      return false;
    }
    
    Pipe pipe = (Pipe) obj;
    return compareTo(pipe) == 0;
  }

  public int compareTo(Pipe anotherPipe) {
    return (int) (this.pipeNo - anotherPipe.pipeNo);
  }

  public void setListenPortNumber(int port) {
    this.ctx.port = port;
    G_PORT = port;
  }

  public void setEventHandler(PipeEventHandler handler) {
    this.eventHandler = handler;
  }

  public void init() throws IOException {
    this.state.init(ctx);
  }

  public String getPipeKey() {
    return pipeKey;
  }

  public boolean write(SelectableChannel ch) throws IOException {
    try {
      return ctx.isPreviousChannel(ch) ? state.writeToPrev(ctx) : state.writeToNext(ctx);
    } finally {
      touch();
    }
  }

  public boolean read(SelectableChannel ch) throws IOException {
    try {
      if (ch == ctx.signalReceiver) {
        return state.writeFileDone(ctx);
      }
      return ctx.isPreviousChannel(ch) ? state.readFromPrev(ctx) : state.readFromNext(ctx);
    } catch (PipeClosing e) {
      this.state = CloseState.instance();
      LOG.debug("Change to close state");
      this.state.readFromPrev(ctx);
      return true;
    } catch (IOException e) {
      if (e instanceof PipeNormallyClosed) {
        LOG.debug("Pipe#" + pipeKey + " is gracefully closed");
      } else {
        if (ctx.isPreviousChannel(ch)) {
          LOG.warn("Pipe " + this + " reading from previous channel is fail : " + e);
        } else {
          LOG.warn("Pipe " + this + " reading from next channel is fail : " + e);
        }
      }
      
      throw e;
    } finally {
      touch();
    }
  }

  public boolean connect(SelectableChannel ch) throws IOException {
    LOG.debug("Connect event is arrived");
    return state.connectToNext(ctx);
  }

  public boolean isCloseState() {
    return state.getStateName().equals("Close State");
  }

  public void close() {
    if (activated.get() == false) {
      LOG.warn("Pipe#" + pipeKey + " is already closed");
      return;
    }

    try {
      eventHandler.pipeClosed(pipeKey);
      eventHandler = null;
    } catch (IOException e) {
      LOG.warn("calling pipe removed event handler is fail", e);
    }

    ctx.close();
//    ctx = null;

    activated.set(false);
  }

  public void changeState(PipeState newState) throws IOException {
    synchronized (stateChangeLock) {
//      this.state.shutdown(ctx);
      newState.init(ctx);
      this.state = newState;

      stateChangeLock.notifyAll();
    }
  }

  public boolean isExpired(long processTime) {
    if (ctx.checkValidity) {
      return System.currentTimeMillis() - lastTouchTime > pipeLifeTime + processTime;
    }
    
    return false;
  }

  public void touch() {
    lastTouchTime = System.currentTimeMillis();
  }

  public String toString() {
    return pipeKey
      + " state : " + ((state == null) ? "NULL" : state.getStateName())
      + ", inReceivingData : " + ctx.status.inReceivingData
      + ", dataReceived : " + ctx.status.dataReceived
      + ", scheduled : " + ctx.status.scheduled
      + ", writingDone : " + ctx.status.writingDone
      + ", ackReceived : " + ctx.status.ackReceived
      + ", acked : " + ctx.status.ackSent
      + " from [" + ((ctx.prevChannel == null) ? "NULL" : ctx.prevChannel.socket().getRemoteSocketAddress()) 
      + "] to [" + ((ctx.nextChannel == null) ? "NULL" : ctx.nextChannel.socket().getRemoteSocketAddress())
      + "]";
  }

  public void register(Selector selector, AsyncFileWriter writer) throws IOException {
    ctx.set(selector, writer);
  }

  class Context {
    class Status {
      boolean inReceivingData = false;
      boolean dataReceived = false;
      //boolean scheduling = false;
      boolean scheduled = false;
      boolean writingDone = false;
      boolean ackSent = false;
      boolean ackReceived = false;
      boolean readyToDisconnect = false;
    }

    Status status = new Status();
    
    Selector selector;
    AsyncFileWriter asyncWriter;
    SocketChannel prevChannel;
    SocketChannel nextChannel;
    SourceChannel signalReceiver;
    SinkChannel signalSender;
    boolean isLastPipe = false;

    Message msg = new Message();
    Bulk bulk;
    
    ByteBuffer ack;

    boolean selectingWriteNext;
    boolean selectingWritePrev;

//    Object numReadingBytesLock = new Object();
//    int numReadingBytes;;
    int port;
    
    boolean checkValidity = true; 
    
    int closeMessagePos = 0;
    final String logPath;
    final String fileName;
    IOException writingError;

    // test
    public long startReceivingTime;

    public long endReceivingTime;

    public long writeDoneTime;

    public Context(SocketChannel prevChannel, String logPath, String fileName) throws IOException {
      this.prevChannel = prevChannel;
      this.logPath = logPath;
      this.fileName = fileName;
      this.ack = ByteBuffer.allocate(PipeState.ACK_SIZE);
      this.ack.clear();

      java.nio.channels.Pipe writeSignal = java.nio.channels.Pipe.open();
      this.signalSender = writeSignal.sink();
      this.signalReceiver = writeSignal.source();
      
      signalSender.configureBlocking(false);
      signalReceiver.configureBlocking(false);
    }

    public void set(Selector selector, AsyncFileWriter writer) throws ClosedChannelException {
      ctx.selector = selector;
      ctx.asyncWriter = writer;
      SelectionKey key = signalReceiver.register(selector, SelectionKey.OP_READ);
      key.attach(Pipe.this);
    }

    public void close() {
      clear();
      
      msg = null;
      if (bulk != null) {
        bulk.close();
      }
      bulk = null;
      prevChannel = null;
      nextChannel = null;
      
      try {
        this.deregisterFromSelect(signalReceiver, SelectionKey.OP_READ);
      } catch (IOException e) {
        LOG.warn("fail to deregister signal receiver from select, but continue, " + e);
      }
      
      try {
        signalReceiver.close();
        signalSender.close();
      } catch(IOException e) {
        LOG.warn("Exception in closing internal pipes, " + e);
      }
    }

    private void clear() {
      msg.clear();
      if (bulk != null) {
        bulk.clear();
      }

      try {
        prevChannel.close();
      } catch (IOException e) {
        LOG.warn("closing previous channel is fail", e);
      }

      try {
        if (nextChannel != null)
          nextChannel.close();
      } catch (IOException e) {
        LOG.warn("closing next channel is fail", e);
      }

//      numReadingBytes = 0;
      isLastPipe = false;
      selector = null;
    }

    boolean isPreviousChannel(SelectableChannel ch) {
      return ch == prevChannel;
    }

    void prepare(PipeConnectionInfo pipeInfo, int pipePosition) throws IOException {
      pipeKey = pipeInfo.getPipeKey();
      eventHandler.pipeEstablished(pipeKey, Pipe.this);
      LOG.debug("New pipe# " + pipeKey + ", pipe pos : " + pipePosition );
    }

    void registerToSelect(SelectableChannel ch, int opCode) throws IOException {
      SelectionKey key = ch.keyFor(selector);

      if (key == null) {
        key = ch.register(selector, opCode);
      } else if (key.isValid() == false) {
        key = ch.register(selector, opCode);
      } else {
        key.interestOps(key.interestOps() | opCode);
      }
      key.attach(Pipe.this);
    }

    public void deregisterFromSelect(SelectableChannel ch, int opCode) throws IOException {
      SelectionKey key = null;

      if ((key = ch.keyFor(selector)) != null) {
        key.interestOps(key.interestOps() & ~opCode);
      }
    }
    
    public void deregisterFromSelectAll(SelectableChannel ch) {
      SelectionKey key = null;

      if ((key = ch.keyFor(selector)) != null) {
        key.interestOps(0);
        key.attach(null);
        key.cancel();
      }
    }

    public void changePipeState(PipeState state) throws IOException {
      Pipe.this.changeState(state);
    }

    public String getMyInetAddress() {
      try {
        return InetAddress.getLocalHost().getHostAddress() + ":" + port;
      } catch (UnknownHostException e) {
        LOG.warn("Cannot reteive the local ip address");
      }

      return "127.0.0.1:" + port;
    }

    public void scheduleWriting() throws IOException {
      asyncWriter.schedule(this);
    }

    public String getPipeKey() {
      return Pipe.this.pipeKey;
    }
  }
}
