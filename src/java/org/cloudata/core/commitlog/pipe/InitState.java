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
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.commitlog.CommitLogServerIF;
import org.cloudata.core.commitlog.UnmatchedLogException;
import org.cloudata.core.commitlog.pipe.Pipe.Context;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.testhelper.FaultInjectionProxy;
import org.cloudata.core.common.testhelper.ProxyExceptionHelper;


public class InitState extends PipeState implements InitStateTestIF {
  static final Log LOG = LogFactory.getLog(InitState.class);
  static final int CONNECTION_TIMEOUT = 5000; // 5 sec

  private static PipeState singleton = new InitState();

  public static PipeState instance() {
    return singleton;
  }

  InitStateTestIF testProxy;
  
  private InitState() {
    testProxy = this;
    
    if (CommitLogServerIF.IS_TEST_MODE) {
      testProxy = FaultInjectionProxy.wrap(this, InitStateTestIF.class);
    }
  }

  public void init(Pipe.Context ctx) throws IOException {
    // LOG.debug("INIT STATE!!!");
    ctx.msg.clear();
    ctx.prevChannel.configureBlocking(false);
    ctx.registerToSelect(ctx.prevChannel, SelectionKey.OP_READ);
  }
  
  public void close(Pipe.Context ctx) throws IOException {
    ctx.msg.clear();
    ctx.deregisterFromSelectAll(ctx.prevChannel);
  }

  public boolean readFromPrev(Pipe.Context ctx) throws IOException {
    if (!ctx.msg.read(ctx.prevChannel)) {
      return false;
    }
    
    PipeConnectionInfo connInfo = PipeConnectionInfo.decode(ctx.msg.getBuffer());
    
    int pipePosition = 0; 
    
    try {
      pipePosition = testProxy.processPipeConnection(ctx, connInfo);
    } catch(UndeclaredThrowableException e) {
      ProxyExceptionHelper.handleException(e, LOG);
    }

    String nextAddr = connInfo.nextAddress(pipePosition);

    LOG.debug("pipeKey : " + connInfo.pipeKey + ", pipePos : " 
        + pipePosition + ", nextAddr : " + nextAddr);
    
    if (nextAddr != null) {
      prepareNextConnection(ctx, nextAddr);
    } else {
      ctx.isLastPipe = true;
      LOG.debug("connection establishment OK");
      sendOkToPrev(ctx);
    }

    return true;
  }

  public int processPipeConnection(Pipe.Context ctx, PipeConnectionInfo connInfo)
      throws IOException, UnmatchedLogException {
    int pipePosition;
    
    if ((pipePosition = connInfo.find(ctx.getMyInetAddress())) < 0) {
      LOG.warn("The address of this host is not in the list. Deny request connecting pipe.");
      LOG.warn("localhost : " + ctx.getMyInetAddress());
      throw new IOException("Wrong address in pipe info");
    }
    
    ctx.prepare(connInfo, pipePosition);

    return pipePosition;
  }

  private void sendOkToPrev(Pipe.Context ctx) throws IOException {
    // LOG.debug("init.sendOkToPrev");
    ctx.msg.clear();
    ctx.msg.setMessage(0, Constants.PIPE_CONNECTED);
    writeToPrev(ctx);
  }

  private void sendErrorToPrev(Context ctx, String msg) throws IOException {
    // LOG.debug("init.sendErrorToPrev");
    ctx.msg.clear();
    ctx.msg.setMessage(0, msg);
    writeToPrev(ctx);
  }

  private void prepareNextConnection(Pipe.Context ctx, String nextAddress) throws IOException {
    ctx.nextChannel = SocketChannel.open();
    ctx.nextChannel.socket().setTcpNoDelay(true);

    ctx.nextChannel.configureBlocking(false);
    ctx.nextChannel.socket().setSoTimeout(CONNECTION_TIMEOUT);

    ctx.nextChannel.connect(new InetSocketAddress(getIP(nextAddress), getPort(nextAddress)));

    connectToNext(ctx);
  }

  public boolean connectToNext(Pipe.Context ctx) throws IOException {
    if (ctx.nextChannel.finishConnect()) {
      ctx.deregisterFromSelect(ctx.nextChannel, SelectionKey.OP_CONNECT);
      ctx.registerToSelect(ctx.nextChannel, SelectionKey.OP_READ);
      writeToNext(ctx);
      return true;
    } else {
      ctx.registerToSelect(ctx.nextChannel, SelectionKey.OP_CONNECT);
      return false;
    }
  }

  public boolean readFromNext(Pipe.Context ctx) throws IOException {
    boolean done = ctx.msg.read(ctx.nextChannel);
    if (done) {
      byte[] bufArray = new byte[ctx.msg.buffer.limit()];
      ctx.msg.buffer.get(bufArray);
      ctx.msg.buffer.rewind();
      String ret = new String(bufArray);

      if (ret.startsWith("OK") == false) {
        LOG.warn("connection establishment is fail due to [" + ret + "]");
      }

      writeToPrev(ctx);

      if (ret.startsWith("OK") == false) {
        throw new IOException("Error due to [" + ret + "]");
      }
      
      LOG.debug("connection establishement OK");
    }
    return done;
  }

  public boolean writeToPrev(Pipe.Context ctx) throws IOException {
    if (ctx.msg.write(ctx.prevChannel)) {
      ctx.deregisterFromSelect(ctx.prevChannel, SelectionKey.OP_WRITE);
      ctx.changePipeState(RunState.instance());
      return true;
    } else {
      ctx.registerToSelect(ctx.prevChannel, SelectionKey.OP_WRITE);
      return false;
    }
  }

  public boolean writeToNext(Pipe.Context ctx) throws IOException {
    if (ctx.msg.write(ctx.nextChannel)) {
      try {
        testProxy.clearWriteProcess(ctx);
      } catch (UndeclaredThrowableException e) {
        ProxyExceptionHelper.handleException(e, LOG);
      }
      return true;
    } else {
      ctx.registerToSelect(ctx.nextChannel, SelectionKey.OP_WRITE);
      return false;
    }
  }

  public void clearWriteProcess(Pipe.Context ctx) throws IOException {
    ctx.deregisterFromSelect(ctx.nextChannel, SelectionKey.OP_WRITE);
    ctx.msg.clear();
  }

  private int getPort(String addr) {
    return Integer.valueOf(addr.substring(addr.indexOf(":") + 1));
  }

  private String getIP(String addr) {
    return addr.substring(0, addr.indexOf(":"));
  }

  public String getStateName() {
    return "Init State";
  }
}
