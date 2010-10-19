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
import java.nio.channels.SelectionKey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.commitlog.pipe.Pipe.Context;
import org.cloudata.core.common.CStopWatch;



abstract class PipeState {
  Log LOG = LogFactory.getLog(PipeState.class);
  static final int ACK_SIZE = 1 + (Long.SIZE / 8);
  
  protected boolean writeAckToPrev(Pipe.Context ctx) throws IOException {
    ctx.prevChannel.write(ctx.ack);

    if (ctx.ack.hasRemaining()) {
      if (!ctx.selectingWritePrev) {
        ctx.registerToSelect(ctx.prevChannel, SelectionKey.OP_WRITE);
        ctx.selectingWritePrev = true;
      }

      LOG.info("ack will be sent in next select time!!");
      return false;
    } else {
      ctx.ack.clear();
      
      if (ctx.selectingWritePrev) {
        ctx.deregisterFromSelect(ctx.prevChannel, SelectionKey.OP_WRITE);
      }
      
      return true;
    }
  }
  
  abstract void init(Pipe.Context ctx) throws IOException;

  abstract String getStateName();

  boolean writeToNext(Pipe.Context ctx) throws IOException {
    LOG.warn("Illegal invocation of writeToNext");
    return true;
  }

  boolean writeToPrev(Pipe.Context ctx) throws IOException {
    LOG.warn("Illegal invocation of writeToPrev");
    return true;
  }

  boolean readFromNext(Pipe.Context ctx) throws IOException {
    LOG.warn("State : " + getStateName() + ", Illegal invocation of readFromNext");
    return true;
  }

  boolean readFromPrev(Pipe.Context ctx) throws IOException {
    LOG.warn("Illegal invocation of readFromPrev");
    return true;
  }

  boolean connectToNext(Pipe.Context ctx) throws IOException {
    LOG.warn("Illegal invocation of connectToNext");
    return true;
  }

  boolean accept(Pipe.Context ctx) {
    LOG.warn("Illegal invocation of accept");
    return true;
  }

  void shutdown(Pipe.Context ctx) {
    LOG.warn("Illegal invocation of shutdown");
  }

  abstract public void close(Pipe.Context ctx) throws IOException;

  public boolean writeFileDone(Context ctx) throws IOException {
    LOG.warn("Illegal invocation of writeFileDone");
    return true;
  }
}
