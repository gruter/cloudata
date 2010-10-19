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
import java.nio.channels.SocketChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

class Message {
  static final Log LOG = LogFactory.getLog(Message.class);

  static final int HEADER_SIZE = 16;
  static final byte[] MAGIC_KEY = new byte[] { 0xC, 0xA, 0xF, 0xE, 0xB, 0xA, 0xB, 0xE };

  ByteBuffer headerBuf;
  ByteBuffer buffer;
  int opCode;
  int length;
  boolean readingHeaderPhase;
  boolean writingHeaderPhase;

  int writtenPosition = 0;

  Message() {
    headerBuf = ByteBuffer.allocate(HEADER_SIZE);
    clear();
  }

  public void setMessage(int opCode, String msg) {
    headerBuf.clear();
    headerBuf.put(MAGIC_KEY);
    headerBuf.putInt(opCode);
    byte[] msgBuf = msg.getBytes();
    headerBuf.putInt(msgBuf.length);
    buffer = ByteBuffer.allocate(msgBuf.length);
    buffer.put(msgBuf);
    headerBuf.flip();
    buffer.flip();
  }

  public ByteBuffer getHeaderBuffer() {
    return headerBuf;
  }

  public ByteBuffer getBuffer() {
    return buffer;
  }

  public boolean write(SocketChannel channel) throws IOException {
    if (writingHeaderPhase) {
      ByteBuffer dupHeaderBuf = duplicate(headerBuf);
      dupHeaderBuf.position(writtenPosition);
      dupHeaderBuf.limit(headerBuf.capacity());
      channel.write(dupHeaderBuf);

      if (dupHeaderBuf.hasRemaining()) {
        writtenPosition = dupHeaderBuf.position();
        return false;
      }
      dupHeaderBuf.clear();
      writtenPosition = 0;
      writingHeaderPhase = false;
    }

    ByteBuffer dupBuffer = duplicate(buffer);
    dupBuffer.position(writtenPosition);
    dupBuffer.limit(buffer.capacity());
    channel.write(dupBuffer);
    
    if (dupBuffer.hasRemaining()) {
      writtenPosition = dupBuffer.position();
      return false;
    }

    writtenPosition = 0;
    writingHeaderPhase = true;
    dupBuffer.clear();
    return true;
  }

  public boolean read(SocketChannel channel) throws IOException {
    if (readingHeaderPhase) {
      if (channel.read(headerBuf) == -1) {
        throw new IOException("End of stream");
      }

      if (headerBuf.hasRemaining()) {
        return false;
      }

      decodeHeader();
      buffer = ByteBuffer.allocate(length);
      readingHeaderPhase = false;
    }

    if (channel.read(buffer) == -1) {
      throw new IOException("End of stream");
    }

    if (!buffer.hasRemaining()) {
      buffer.flip();
      readingHeaderPhase = true;
      return true;
    }

    return false;
  }

  private void decodeHeader() throws IOException {
    headerBuf.flip();

    if (hasValidHeader(headerBuf)) {
      headerBuf.position(MAGIC_KEY.length);

      opCode = headerBuf.getInt();
      length = headerBuf.getInt();
    } else {
      throw new IOException("Invalid connection with invalid header");
    }
  }

  private boolean hasValidHeader(ByteBuffer buf) {
    byte[] array = buf.array();

    for (int i = 0; i < MAGIC_KEY.length; i++) {
      if (array[i] != MAGIC_KEY[i]) {
        LOG.warn("Invalid header");
        return false;
      }
    }
    LOG.debug("Valid header");
    return true;
  }

  private ByteBuffer duplicate(ByteBuffer buf) {
    // LOG.debug("before pos : " + buf.position());
    // LOG.debug("before limit : " + buf.limit());

    ByteBuffer ret = buf.duplicate();

    if (ret.position() == ret.limit()) {
      ret.flip();
    }

    // LOG.debug("after pos : " + ret.position());
    // LOG.debug("after limit : " + ret.limit());

    return ret;
  }

  public void clear() {
    headerBuf.clear();
    buffer = null;
    readingHeaderPhase = true;
    writingHeaderPhase = true;
    writtenPosition = 0;
  }
}
