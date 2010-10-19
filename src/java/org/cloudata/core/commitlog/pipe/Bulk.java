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
import java.nio.channels.SocketChannel;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.commitlog.CommitLogServerIF;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.testhelper.FaultInjectionProxy;
import org.cloudata.core.common.testhelper.ProxyExceptionHelper;


public class Bulk implements BulkTestIF {
  public static final int DEFAULT_BUFFER_SIZE = 256 * 1024; // 256KB
  static final Log LOG = LogFactory.getLog(Bulk.class);

  static enum OperationResult {
    completed, partially
  }

  ArrayList<ByteBuffer> bufferList = new ArrayList<ByteBuffer>(10);
  int currentReadBufIndex = 0;
  int currentWriteBufIndex = 0;
  int writtenPos = 0;
  byte[] txId = null;
  int seq = -1;
  int headerAndPayloadSize = 0;
  boolean readingHeaderDone = false;
  int totalNumRead = 0;
  int readBytesLength = 4;

  boolean printNumWritten = false;
  int totalNumWritten = 0;

  BulkTestIF testProxy;
  volatile byte[] dirNameBytes;
  
  public Bulk() {
    testProxy = this;
    if (CommitLogServerIF.IS_TEST_MODE) {
      testProxy = FaultInjectionProxy.wrap(this, BulkTestIF.class);
    }
    addNewBuffersToList();
  }

  public OperationResult read(SocketChannel ch) throws IOException {
    if (bufferList.isEmpty()) {
      throw new IOException("Pipe is closed");
    }

    while (true) {
      ByteBuffer buf = bufferList.get(currentReadBufIndex);
      int numRead = 0;
      try {
        if ((numRead = testProxy.readFromChannel(ch, buf)) < 0) {
          throw new IOException("Disconnect from previous");
        }
      } catch(UndeclaredThrowableException e) {
        ProxyExceptionHelper.handleException(e, LOG);
      }
      
      LOG.debug("num read : " + numRead);
      totalNumRead += numRead;

      if (!readingHeaderDone) {
        readingHeaderDone = readHeader(buf);
      }

      if (totalNumRead >= headerAndPayloadSize) {
        if (totalNumRead > headerAndPayloadSize) {
          LOG.warn("buffer over read. totalNumRead : " + totalNumRead 
              + ", but expected size : " + headerAndPayloadSize);
        }

        LOG.debug("total num read : " + totalNumRead + ", expected : " + headerAndPayloadSize);

        return OperationResult.completed;
      }
      
      if (buf.hasRemaining()) {
        LOG.debug("total num read : " + totalNumRead + ", expected : " + headerAndPayloadSize);
        
        return OperationResult.partially;
      }

      currentReadBufIndex++;
      LOG.debug("buffer commit, index[" + currentReadBufIndex + "]");

      if (bufferList.size() <= currentReadBufIndex) {
        addNewBuffersToList();
        LOG.debug("bulk [" + this + "] append new buffer. bufferList.size : " + bufferList.size());
      }
    }
  }

  // by sangchul test때문에 visibility를 public으로 변경
  public int readFromChannel(SocketChannel ch, ByteBuffer buf) throws IOException {
    int curNumRead;
    if ((curNumRead = ch.read(buf)) < 0) {
      throw new IOException("End of stream");
    }
    return curNumRead;
  }
  
  public String getDirName() {
    return new String(dirNameBytes);
  }

  private boolean readHeader(ByteBuffer buf) throws IOException {
    ByteBuffer headerBuf = buf.duplicate();

    if (seq < 0 && headerBuf.limit() >= readBytesLength) {
      headerBuf.position(0);
      seq = headerBuf.getInt();

      if (seq == Constants.PIPE_DISCONNECT) {
        LOG.debug("receive PIPE_DISCONNECT");
        throw new PipeClosing();
      }
      
      readBytesLength += 4;
    }
    
    if (dirNameBytes == null && headerBuf.limit() >= readBytesLength) {
      headerBuf.position(readBytesLength - 4);
      int len = headerBuf.getInt();
      if (len > 1000000) {
        throw new IOException("dirName byte length is too long [" + len + "]");
      }
      
      dirNameBytes = new byte[len];
      readBytesLength += len;
    }
    
    if (dirNameBytes != null && headerBuf.limit() >= readBytesLength) {
      headerBuf.position(readBytesLength - dirNameBytes.length);
      headerBuf.get(dirNameBytes);
      readBytesLength += 4;
    }
    
    if (headerAndPayloadSize == 0 && headerBuf.limit() >= readBytesLength) {
      headerBuf.position(readBytesLength - 4);
      headerAndPayloadSize = headerBuf.getInt();

      return true;
    }

    return false;
  }

  public OperationResult write(SocketChannel ch) throws IOException {
    if (bufferList.size() == 0) {
      throw new IOException("Pipe is closed");
    }

    int numWritten = 0;

    while (true) {
      ByteBuffer readBuf = bufferList.get(currentWriteBufIndex);
      ByteBuffer writeBuffer = readBuf.duplicate();
      writeBuffer.position(writtenPos);
      writeBuffer.limit(readBuf.position());

      numWritten = ch.write(writeBuffer);

      writtenPos += numWritten;
      totalNumWritten += numWritten;

      if (writeBuffer.hasRemaining()) {
        return OperationResult.partially;
      }

      //LOG.info("totalNumWritten : " + totalNumWritten + ", totalNumRead : " + totalNumRead);
      if (totalNumWritten < totalNumRead) {
        if (currentWriteBufIndex < currentReadBufIndex) {
          currentWriteBufIndex++;
          writtenPos = 0;
        } else {
          return OperationResult.partially;
        }
      } else {
        return OperationResult.completed;
      }
    }
  }

  // by sangchul
  // 이 메소드는 pipe를 다루는 쓰레드와 다른 쓰레드가 들어온다.
  // 하지만, 그 다른 쓰레드는 pipe에 이미 정해진 수의 바이트가 들어오기 전까진 block하고 있다가
  // 그 후에 이 메소드를 호출하기 때문에 bufferList가 여러 쓰레드에 의해 공유되는 상황은 발생하지 않는다.
  public ByteBuffer[] getBufferArray() {
    ByteBuffer[] bufferArray = bufferList.toArray(new ByteBuffer[0]);

    for (int i = 0; i < bufferArray.length; i++) {
      bufferArray[i].flip();
    }

    return bufferArray;
  }

  public void clear() {
    ByteBuffer buf = bufferList.remove(bufferList.size() - 1);
    buf.clear();
    
    if (bufferList.isEmpty() == false) {
      LOG.debug("bulk [" + this + "] clear, " + bufferList.size() + " is returned");
      BufferPool.singleton().returnBuffer(bufferList.toArray(new ByteBuffer[0]));
    }

    bufferList.clear();
    bufferList.add(buf);
    
    currentReadBufIndex = 0;
    currentWriteBufIndex = 0;
    writtenPos = 0;
    readingHeaderDone = false;
    headerAndPayloadSize = 0;
    totalNumRead = 0;
    txId = null;
    seq = -1;
    totalNumWritten = 0;
    readBytesLength = 4;
    dirNameBytes = null;
  }

  private void returnBuffer() {
    ByteBuffer[] bufferArray = bufferList.toArray(new ByteBuffer[0]);
    bufferList.clear();
    BufferPool.singleton().returnBuffer(bufferArray);
  }

  private void addNewBuffersToList() {
    for (ByteBuffer buf : BufferPool.singleton().getBuffer(DEFAULT_BUFFER_SIZE)) {
      LOG.debug("add new buffer");
      buf.clear();
      bufferList.add(buf);
    }
  }

  public void close() {
    returnBuffer();
    bufferList = null;
  }

  public boolean isWrittingDone() {
    return totalNumRead <= totalNumWritten;
  }
}
