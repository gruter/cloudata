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
package org.cloudata.core.tabletserver;

import org.cloudata.core.common.util.StringUtils;

public abstract class AsyncTask implements Runnable {
  String taskId;
  AsyncTaskStatus status;
  
  public AsyncTask() {
    status = new AsyncTaskStatus();
  }
  
  public String getTaskId() {
    return taskId;
  }
  
  public void run() {
    try {
      exec();
    } catch (Exception e) {
      status.error = true;
      if(status.errorTrace != null && status.errorTrace.length() > 0) {
        status.errorTrace += "===============================\n";
      }
      status.errorTrace += StringUtils.stringifyException(e);
    } finally {
      status.end = true;
    }
  }
  
  public void addErrorTrace(String message) {
    if(status.errorTrace != null && status.errorTrace.length() > 0) {
      status.errorTrace += "===============================\n";
    }
    status.error = true;
    status.errorTrace += message;
  }
  public abstract void exec() throws Exception;
}
