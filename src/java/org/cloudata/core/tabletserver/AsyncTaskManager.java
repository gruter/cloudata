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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AsyncTaskManager {
  //TaskId -> AsyncTask
  protected Map<String, AsyncTask> runningTasks = new HashMap<String, AsyncTask>();
  
  protected ExecutorService asyncExecutor = Executors.newFixedThreadPool(20);
  
  public String runAsyncTask(AsyncTask task) {
    synchronized(runningTasks) {
      String taskId = task.getClass().getName() + System.currentTimeMillis();
      
      //이미 존재하는 id인 경우 다시 생성
      if(runningTasks.containsKey(taskId)) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          return null;
        }
        taskId = task.getClass().getName() + System.currentTimeMillis();
      }
      
      //task 실행
      task.taskId = taskId;
      runningTasks.put(task.taskId, task);
      asyncExecutor.execute(task);
      return taskId;
    }
  }

  public void removeAsyncTask(String taskId) {
    synchronized(runningTasks) {
      runningTasks.remove(taskId);
    }
  }
  
  public AsyncTaskStatus getAsyncTaskStatus(String taskId) {
    synchronized(runningTasks) {
      AsyncTask task = runningTasks.get(taskId);
      if(task == null) {
        return null;
      } else {
        if(task.status.end) {
          runningTasks.remove(taskId);
        }
        return task.status;
      }
    }
  }

  public AsyncTask getAsyncTask(String taskId) {
    synchronized(runningTasks) {
      return runningTasks.get(taskId);
    }
  }
}
