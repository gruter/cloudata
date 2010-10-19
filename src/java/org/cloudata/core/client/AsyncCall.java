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
package org.cloudata.core.client;

import java.io.IOException;

import org.cloudata.core.common.AsyncCallProtocol;
import org.cloudata.core.tabletserver.AsyncTaskStatus;


public abstract class AsyncCall {
  public abstract String run() throws IOException;
  
  public void call(String callName, AsyncCallProtocol server, int timeout) throws IOException {
    String taskId = run();
    long startTime = System.currentTimeMillis();
    try {
      while(true) {
        try {
          AsyncTaskStatus task = server.getAsyncTaskStatus(taskId);
          if(task != null) {
            if(task.isEnd()) {
              if(task.isError()) {
                throw new IOException(task.getErrorTrace());
              } else {
                break;
              }
            }
          }
          
          try {
            Thread.sleep(2 * 1000);
          } catch (InterruptedException ie) {
            return;
          }
          
          long gap = System.currentTimeMillis() - startTime;
          if(gap > timeout) {
            throw new IOException("Timeout:" + callName + " after " + gap + " ms");
          }
          
        } catch (Exception e) {
          IOException err = new IOException(e.getMessage());
          err.initCause(e);
          throw err;
        }
      }
    } finally {
      server.removeAsyncTask(taskId);
    }
  }
}
