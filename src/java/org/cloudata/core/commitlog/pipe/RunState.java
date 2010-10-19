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
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.commitlog.CommitLogServer;
import org.cloudata.core.commitlog.CommitLogServerIF;
import org.cloudata.core.commitlog.CommitLogShutDownException;
import org.cloudata.core.commitlog.pipe.Bulk.OperationResult;
import org.cloudata.core.commitlog.pipe.Pipe.Context;
import org.cloudata.core.common.testhelper.FaultInjectionProxy;
import org.cloudata.core.common.testhelper.ProxyExceptionHelper;


class RunState extends PipeState implements RunStateTestIF {
  static final Log LOG = LogFactory.getLog(RunState.class);
  private static final PipeState singleton = new RunState();

  public static PipeState instance() {
    return singleton;
  }

  RunStateTestIF testProxy;

  RunState() {
    testProxy = this;
    if (CommitLogServerIF.IS_TEST_MODE) {
      testProxy = FaultInjectionProxy.wrap(this, RunStateTestIF.class);
    }
  }

  public void init(Pipe.Context ctx) throws IOException {
    ctx.registerToSelect(ctx.prevChannel, SelectionKey.OP_READ);
    ctx.prevChannel.socket().setTcpNoDelay(true);
    ctx.prevChannel.configureBlocking(false);

    if (ctx.nextChannel != null) {
      ctx.registerToSelect(ctx.nextChannel, SelectionKey.OP_READ);
    }
    
    ctx.bulk = new Bulk();
  }

  public void close(Context ctx) throws IOException {
    ctx.deregisterFromSelectAll(ctx.prevChannel);
    if (ctx.nextChannel != null) {
      ctx.deregisterFromSelectAll(ctx.nextChannel);
    }
    ctx.bulk = null;
  }

  public boolean readFromNext(Pipe.Context ctx) throws IOException {
    int numRead = ctx.nextChannel.read(ctx.ack);

    if (numRead < 0) {
      throw new IOException("Disconnect from next...");
    }

    if (ctx.ack.hasRemaining()) {
      LOG.warn("reading ack is pending in next select time, numRead :" + numRead);
      return false;
    } else {
//      ctx.ack.flip();
//
//      byte ret = ctx.ack.get();
//      long ackTime = ctx.ack.getLong();
//      long time = System.currentTimeMillis();
//      if ((time - ackTime) > 1000) {
//        LOG.debug("TIME REPORT [too long time to pass ack] takes " + (time - ackTime) + " ms");
//	  }

      ctx.ack.flip();

      LOG.debug("pass ack to previous ctx.pipe : " + ctx.getPipeKey());
      ctx.status.ackReceived = true;

      return writeToPrev(ctx);
    }
  }

  public boolean readFromPrev(Pipe.Context ctx) throws IOException {
    if (ctx.status.dataReceived) {
      ByteBuffer buf = ByteBuffer.allocate(100);
      int numRead = ctx.prevChannel.read(buf);

      if (numRead < 0) {
        throw new IOException("End of Stream");
      }

      LOG.warn("Duplicated reception of data from previous! numRead : " + numRead);
      dump(buf);
      throw new IOException("Duplicated reception of data from previous!");
    }

    OperationResult ret = ctx.bulk.read(ctx.prevChannel);

    if (ctx.status.inReceivingData == false) {
      ctx.status.inReceivingData = true;
      ctx.status.dataReceived = false;
      ctx.status.writingDone = false;
      ctx.status.ackReceived = false;
      ctx.status.ackSent = false;

      // pipe의 validity를 체크해서 timeout동안 데이터가 수신되지 않으면
      // 끊어버린다.
      ctx.checkValidity = true;
      ctx.startReceivingTime = System.currentTimeMillis();
    }

    if (ret == OperationResult.completed) {
      ctx.endReceivingTime = System.currentTimeMillis();
      
      if (ctx.endReceivingTime - ctx.startReceivingTime > 100) {
        LOG.info("TIME REPORT [too long time to transfer] takes " + (ctx.endReceivingTime - ctx.startReceivingTime) + "ms");
      }
      
      ctx.status.inReceivingData = false;
      ctx.status.dataReceived = true;
    }

    writeToNext(ctx);

    return true;
  }

  public boolean writeToNext(Pipe.Context ctx) throws IOException {
    if (ctx.isLastPipe && ctx.status.dataReceived) {
      if (ctx.status.scheduled == false) {
        try {
          testProxy.writeToLogFile(ctx);
        } catch (UndeclaredThrowableException e) {
          LOG.error("error in scheduling write", e);
          ProxyExceptionHelper.handleException(e, LOG);
        }
      } else {
        LOG.warn("tried to schedule again.");
      }

      return true;
    }

    if (ctx.nextChannel != null && ctx.bulk.isWrittingDone() == false) {
      if (ctx.bulk.write(ctx.nextChannel) == OperationResult.partially) {
        if (ctx.selectingWriteNext == false) {
          ctx.registerToSelect(ctx.nextChannel, SelectionKey.OP_WRITE);
          ctx.selectingWriteNext = true;
        }

        return false;
      } else if (ctx.status.inReceivingData == false && ctx.bulk.isWrittingDone()) {
        if (ctx.status.scheduled == false) {
          try {
            testProxy.writeToLogFile(ctx);
          } catch (UndeclaredThrowableException e) {
            LOG.error("error in scheduling write", e);
            ProxyExceptionHelper.handleException(e, LOG);
          }
        } else {
          LOG.warn("tried to schedule again..");
        }

        if (ctx.selectingWriteNext) {
          ctx.deregisterFromSelect(ctx.nextChannel, SelectionKey.OP_WRITE);
          ctx.selectingWriteNext = false;
        }
      }
    }

    return true;
  }

  public void writeToLogFile(Pipe.Context ctx) throws IOException {
    try {
      if (ctx.status.scheduled) {
        LOG.warn("WARN!!! ctx is already scheduled pipe key : " + ctx.getPipeKey());
      }

      ctx.scheduleWriting();
      ctx.status.scheduled = true;
    } catch (IOException e) {
      LOG.warn("scheduling writing is fail", e);
      throw e;
    }
  }

  @Override
  public boolean writeFileDone(Context ctx) throws IOException {
    ctx.status.scheduled = false;
    ctx.status.writingDone = true;
    ByteBuffer ret = ByteBuffer.allocate(9);

    if (ctx.signalReceiver.read(ret) < 0) {
      throw new IOException("Internal pipe disconnection");
    }

    ret.flip();
    ctx.bulk.clear();
    
    byte result = ret.get();
    long time = ret.getLong();
    
    ctx.writeDoneTime = System.currentTimeMillis();
    
    CommitLogServer.metrics.setWrite(ctx.bulk.totalNumWritten, (ctx.writeDoneTime - time));
    
    if (ctx.writeDoneTime - time > 100) {
      LOG.info("TIME REPORT [too long time to be signaled] takes " + (ctx.writeDoneTime - time) + "ms");
    }

    if (result == AsyncFileWriter.NACK) {
      writeFileFail(ctx);
    }

    if (ctx.isLastPipe) {
      ctx.ack.clear();
      ctx.ack.put(AsyncFileWriter.ACK);
      ctx.ack.putLong(System.currentTimeMillis());
      ctx.ack.flip();
      writeToPrev(ctx);
    } else if (ctx.status.ackReceived) {
      writeToPrev(ctx);
    }

    return true;
  }

  private void writeFileFail(Context ctx) throws CommitLogShutDownException {
    String msg = "FAIL TO WRITE COMMIT LOG, THIS SERVER SHOULD BE SHUTDOWN";
    if (ctx.writingError != null) {
      LOG.fatal(msg, ctx.writingError);
    } else {
      LOG.fatal(msg);
    }

    throw new CommitLogShutDownException(ctx.writingError != null ? ctx.writingError
        : new IOException("Writing fail"));
  }

  public boolean writeToPrev(Pipe.Context ctx) throws IOException {
    if (ctx.status.writingDone == false) {
      return false;
    }

    boolean ret = writeAckToPrev(ctx);

    if (ret) {
      ctx.checkValidity = false;

      ctx.status.inReceivingData = false;
      ctx.status.dataReceived = false;
      ctx.status.scheduled = false;
      ctx.status.writingDone = false;
      ctx.status.ackReceived = false;
      ctx.status.ackSent = true;

      ctx.ack.clear();
    } else {
      LOG.warn("reset in next time");
    }

    return ret;
  }

  public String getStateName() {
    return "Run State";
  }

  static void dump(ByteBuffer buf) {
    buf.flip();
    byte[] array = buf.array();
    for (int i = 0; i < array.length; i++) {
      System.out.format("0x%x ", array[i]);
      if ((i + 1) % 8 == 0) {
        System.out.println();
      }
    }
  }
}
