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
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.commitlog.PipeNormallyClosed;
import org.cloudata.core.commitlog.pipe.Pipe.Context;
import org.cloudata.core.common.Constants;


public class CloseState extends PipeState implements FaultInjectionIF {
  public static final ByteBuffer READY_TO_DISCONNECT_MSG = ByteBuffer.allocate(1);
  Log LOG = LogFactory.getLog(CloseState.class);

  ByteBuffer closeMessage = ByteBuffer.allocate(4);

  public static PipeState instance() {
    return new CloseState();
  }

  CloseState() {
    closeMessage.putInt(Constants.PIPE_DISCONNECT);
    closeMessage.flip();
  }

  public String getStateName() {
    return "Close State";
  }

  public void init(Context ctx) throws IOException {
    LOG.debug("CLOSE STATE!!!");
  }

  public void close(Context ctx) throws IOException {
    LOG.debug("CLOSE STATE!!!");
  }

  private boolean closeGracefully(Context ctx) throws IOException {
//    ByteBuffer buf = ByteBuffer.allocate(1);
//    int numRead = 0;
//    try {
//      numRead = ctx.prevChannel.read(buf);
//    } catch (IOException e) {
//      throw new PipeNormallyClosed();
//    }
//
//    if (numRead < 0) {
//      throw new PipeNormallyClosed();
//    }
//
//    LOG.warn("WHAT THE HELL??");
//    return true;
    throw new PipeNormallyClosed();
  }

  public boolean readFromPrev(Context ctx) throws IOException {
    LOG.debug("readFromPrev");
    
    if (ctx.status.readyToDisconnect) {
      LOG.debug("close gracefully");
      return closeGracefully(ctx);
    }

    return writeToNext(ctx);
  }

  public boolean writeToNext(Context ctx) throws IOException {
    LOG.debug("writeToNext");

    if (!ctx.isLastPipe) {
      LOG.debug("not last pipe");
      ByteBuffer msg = closeMessage.duplicate();
      msg.position(ctx.closeMessagePos);
      try {
        ctx.closeMessagePos = ctx.nextChannel.write(msg);
      } catch(IOException e) {
        throw new PipeNormallyClosed();
      }

      if (msg.hasRemaining()) {
        ctx.deregisterFromSelect(ctx.nextChannel, SelectionKey.OP_WRITE);
        return false;
      }
    } else {
      LOG.debug("send ok ack to prev");
      ctx.closeMessagePos = 0;
      ctx.ack.clear();
      ctx.ack.put(Constants.PIPE_DISCONNECT_OK);
      ctx.ack.flip();
      return writeToPrev(ctx);
    }

    return true;
  }

  public boolean readFromNext(Context ctx) throws IOException {
    ctx.ack.clear();
    boolean disconnected = false;
    try {
      disconnected = ctx.nextChannel.read(ctx.ack) < 0;
    } catch(IOException e) {
      LOG.info("strange close but continue : " + e);
      throw new PipeNormallyClosed();
    }
    
    if (disconnected) {
      LOG.info("strange close but continue");
      throw new PipeNormallyClosed();
    }
    
    ctx.ack.flip();
    ctx.status.readyToDisconnect = writeAckToPrev(ctx);
    return ctx.status.readyToDisconnect;
  }
  
  public boolean writeToPrev(Context ctx) throws IOException {
    LOG.debug("writeToPrev");
    ctx.status.readyToDisconnect = writeAckToPrev(ctx);
    return ctx.status.readyToDisconnect;
  }
}
