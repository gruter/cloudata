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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.common.conf.CloudataConf;


abstract class AbstractRetryCall {
  static final Log LOG = LogFactory.getLog(AbstractRetryCall.class);
  ThreadLocal<RetryFlag> retryFlagLocal = new ThreadLocal<RetryFlag>();
  final int timeout;
  final int sleep;
  final int maxRetryCount;

  AbstractRetryCall(CloudataConf conf) {
    this(conf.getInt("tx.max.retry.count", 0), 
         conf.getInt("tx.retry.sleeptime.msec", 100),
         conf.getInt("client.tx.timeout", 60));
  }

  AbstractRetryCall(int maxRetryCount, int sleepMSec, int txTimeoutSec) {
    this.maxRetryCount = maxRetryCount;
    this.sleep = sleepMSec; // convert sec to msec
    this.timeout = txTimeoutSec * 1000;
  }

  protected abstract void internalProcess(RetryFlag flag) throws IOException;
  
  /**
   * Call을 재시도 하기 전에 호출 된다. Call이 처음 호출 될 때는 실행되지 않는다.
   * @throws IOException
   */
  protected void executeRetryHandler(long remainedTimeMSec, boolean reloadMeta) throws IOException {
  }

  void processCall() throws IOException {
    IOException exception = null;
    long startTime = System.currentTimeMillis();
    int count = 0;
    boolean isTimedOut = false;
    int currentSleep = sleep;
    
    RetryFlag retryFlag = retryFlagLocal.get();
    
    if (retryFlag == null) {
      retryFlagLocal.set(new RetryFlag());
      retryFlag = retryFlagLocal.get();
    }
    
    //retryFlag.reloadMeta = false;
    retryFlag.retry = false;
    
    while(maxRetryCount == 0 || count < maxRetryCount) {
      retryFlag.reloadMeta = false;
      try {
        internalProcess(retryFlag);
        if (!retryFlag.retry) {
          return;
        }
      } catch (IOException e) {
        if (exception != null && e.getMessage() != null && exception.getMessage() != null) {
          if (e.getMessage().equals(exception.getMessage()) == false) {
            LOG.debug("Different exception has occurred. previous exception : " + exception + ", current exception : " + e);
          }
        }
        exception = e;
        retryFlag.reloadMeta = true;
      } 
      
      long remainedTxTime = timeout > 0? timeout - (System.currentTimeMillis() - startTime) : 0;

      if(timeout > 0 && remainedTxTime <= 0) {
        isTimedOut = true;
        break;
      }
 
      executeRetryHandler(remainedTxTime, retryFlag.reloadMeta);

      remainedTxTime = timeout > 0? timeout - (System.currentTimeMillis() - startTime) : 0;

      if(timeout > 0 && remainedTxTime <= 0) {
        isTimedOut = true;
        break;
      }

      String remainedTimeInfo = timeout > 0? " Remained tx time : " + (remainedTxTime) : "";
      LOG.debug("Retry after " + currentSleep + " ms. " + remainedTimeInfo);
       
      if (sleep > 0) {
        try { 
          if(retryFlag != null && retryFlag.sleep > 0) {
            Thread.sleep(retryFlag.sleep); 
          } else {
            Thread.sleep(currentSleep); 
          }
          currentSleep = currentSleep + sleep >= 1000 ? 1000 : currentSleep + sleep;
        } catch(Exception e) {
          System.err.println("break by interrupt");
          return;
        }
      }
      count++;
    }
    
    if(maxRetryCount > 0 && count >= maxRetryCount) {
      if(exception != null) {
        throw exception;
      }

      throw new IOException("Exceed " + maxRetryCount + " times in retrying rpc operation");
    } else if(isTimedOut) {
      throw new IOException("IO operation is timed out " + (timeout / 1000) + " sec" + (exception != null? " due to " + exception.getMessage() : ""));
    }     

    throw new IOException("Fail to operation due to unknown reason");
  }
}


abstract class RetryCallWithoutReturn extends AbstractRetryCall {
  /**
   * 작업을 반복해서 수행한다. 호출에 리턴값이 없을 때 사용.
   * 기본 값으로 다섯 번 반복하며, 반복 수행할 때 1초를 쉬고, 전체 수행시간을 10초를 넘기지 않는다.
   */
  RetryCallWithoutReturn(CloudataConf conf) {
    super(conf);
  }

  /**
   * 작업을 반복해서 수행한다. 호출에 리턴값이 없을 때 사용.
   * @param retryCount RPC 반복 회수. 0일 경우 무한 반복.
   * @param sleepSec RPC 반복 회수. 0일 경우 무한 반복.
   * @param timeoutSec RPC 및 RPC 반복의 한계 시간. 0일 경우 시간 제한을 두지 않고 수행
   */
  RetryCallWithoutReturn(int retryCount, int sleepMSec, int timeoutSec) {
    super(retryCount, sleepMSec, timeoutSec);
  }
  
  protected void internalProcess(RetryFlag doRetry) throws IOException {
    run(doRetry);
  }
  
  public void call() throws IOException {
    processCall();
  }
  
  abstract void run(RetryFlag doRetry) throws IOException;
}


abstract class RetryCall<T> extends AbstractRetryCall {
  T ret;
  
  /**
   * 작업을 반복해서 수행한다. 호출 리턴값은 T로 지정.
   * 기본 값으로 다섯 번 반복하며, 반복 수행할 때 1초를 쉬고, 전체 수행시간을 10초를 넘기지 않는다.
   */
  RetryCall(CloudataConf conf) {
    super(conf);
  }

  /**
   * 작업을 반복해서 수행한다. 호출 리턴값은 T로 지정.
   * @param retryCount RPC 반복 회수. 0일 경우 무한 반복.
   * @param sleepSec RPC 반복 회수. 0일 경우 무한 반복.
   * @param timeoutSec RPC 및 RPC 반복의 한계 시간. 0일 경우 시간 제한을 두지 않고 수행
   */
  RetryCall(int retryCount, int sleepMSec, int timeoutSec) {
    super(retryCount, sleepMSec, timeoutSec);
  }
  
  public T call() throws IOException {
    processCall();
    return ret;
  }

  protected void internalProcess(RetryFlag doRetry) throws IOException {
    ret = run(doRetry);
  }
  
  abstract T run(RetryFlag doRetry) throws IOException;
}

class RetryFlag {
  boolean retry = false;
  long sleep;
  boolean reloadMeta;
}
