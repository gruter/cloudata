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
package org.cloudata.core.common;

import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TimeLog {
  public static final Log LOG = LogFactory.getLog(TimeLog.class.getName());
  
  private static ThreadLocal<TimeLog> threadLocal = new ThreadLocal<TimeLog>();
  
  private Stack<Long> startTimes = new Stack<Long>();
  
  public static void checkPoint() {
    getTimeLog().startTimes.push(System.currentTimeMillis());
  }
  
  public static long endCheckPoint(String message) {
    long elapsedTime = System.currentTimeMillis() - getTimeLog().startTimes.pop();
    //LOG.info(message + ": " + elapsedTime);
    return elapsedTime;
  }
  
  public static long endCheckPoint() {
    return endCheckPoint(null);
  }
  
  private static TimeLog getTimeLog() {
    TimeLog timeLog = threadLocal.get();
    if(timeLog == null) {
      timeLog = new TimeLog();
      threadLocal.set(timeLog);
    }
    
    return timeLog;
  }
  
  public static void checkTime(long startTime, long checkTime, String message) {
    long gap = System.currentTimeMillis() - startTime;
    if(gap > checkTime) {
      LOG.debug(message + ", time=" + gap);
    }
  }
}
