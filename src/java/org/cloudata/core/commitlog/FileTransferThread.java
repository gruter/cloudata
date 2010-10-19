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
import java.nio.channels.FileChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class FileTransferThread extends Thread {
  static final Log LOG = LogFactory.getLog(FileTransferThread.class);

  Selector selector = Selector.open();
  ServerSocketChannel ssc;
  FileChannel[] channelList;
  String dirName;
  
  FileTransferThread(String dirName, ServerSocketChannel ssc, FileChannel[] channelList) throws IOException {
    this.dirName = dirName;
    this.ssc = ssc;
    this.channelList = channelList;
    this.ssc.register(selector, SelectionKey.OP_ACCEPT);
  }

  public void run() {
    SocketChannel socketChannel = null;

    try {
      if (selector.select(5000) > 0) {
        Iterator<SelectionKey> iter = selector.selectedKeys().iterator();

        while (iter.hasNext()) {
          SelectionKey key = iter.next();
          if (key.isValid() && key.isAcceptable()) {
            socketChannel = ssc.accept();

            transfer(channelList, socketChannel);
          }

          iter.remove();
        }
      } else {
        LOG.warn("No responses from client asking to transfter file for 5 sec");
      }
    } catch (IOException e) {
      LOG.warn("transfering file is fail", e);
    } finally {
      if (socketChannel != null) {
        try {
          socketChannel.socket().close();
          socketChannel.close();
        } catch (IOException e) { /* ignored */
        }
      }
    }

    LOG.debug("File transfer thread is done");
  }

  private void transfer(FileChannel[] channelList, SocketChannel socketChannel) {
    for (FileChannel fc : channelList) {
      try {
        long numTransferred = 0;
        long pos = fc.position();
        long totalLength = fc.size() - pos;
        long count = totalLength;

        LOG.info(dirName + " start File Transfer:" + pos + "," + totalLength);
        while ((numTransferred += fc.transferTo(pos, count, socketChannel)) < totalLength) {
          pos += numTransferred;
          count -= numTransferred;
        }
        //LOG.info("End File Transfer:" + pos + "," + totalLength);
      } catch (IOException e) {
        LOG.warn("transfering file is fail due to : ", e);
      } finally {
        if (fc != null) {
          try {
            fc.close();
          } catch (IOException e) {
            LOG.warn("closing file is fail due to : ", e);
          }
        }
      }
    }
  }
}
