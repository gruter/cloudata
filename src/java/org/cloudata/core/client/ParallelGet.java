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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 멀티쓰레드로 여러개의 row를 한번에 조회하는 기능 수행 
 *
 */
class ParallelGet {
  private Iterator<Row.Key> iterator;
  private List<CellFilter> cellFilters;
  private CTable ctable; 
  private AsyncDataReceiver dataReceiver;
  private int timeout;
  private int numOfThread;
  private AtomicInteger remainCount;
  
  private ExecutorService parallelGetExecutor = Executors.newFixedThreadPool(50);
  
  protected ParallelGet(CTable ctable,
      final Collection<Row.Key> rowKeys, 
      final List<CellFilter> cellFilters,
      int numOfThread,
      int timeout,
      AsyncDataReceiver dataReceiver) {
    this.ctable = ctable;
    this.iterator = rowKeys.iterator();
    this.cellFilters = cellFilters;
    this.dataReceiver = dataReceiver;
    this.timeout = timeout;
    
    this.numOfThread = numOfThread;
    if(numOfThread > 200) {
      this.numOfThread = 200;
    }
    
    this.remainCount = new AtomicInteger(rowKeys.size());
  }
  
  public boolean get() throws IOException {
    for(int i = 0; i < numOfThread; i++) {
      parallelGetExecutor.execute(new GetThread());
    }
    long startTime = System.currentTimeMillis();
    synchronized(remainCount) {
      while(true) {
        if(remainCount.get() == 0) {
          break;
        }
        try {
          remainCount.wait(timeout);
        } catch (InterruptedException e) {
          return false;
        }
        if(remainCount.get() == 0) {
          break;
        }
        if((System.currentTimeMillis() - startTime) >= timeout) {
          parallelGetExecutor.shutdownNow();
          dataReceiver.timeout();
          return false;
        }
      }
    }
    
    if(!parallelGetExecutor.isShutdown()) {
      parallelGetExecutor.shutdownNow();
    }
    return true;
  }
    
  class GetThread extends Thread {
    public void run() {
      while(true) {
        Row.Key rowKey = null;
        synchronized(iterator) {
          if(!iterator.hasNext()) {
            synchronized(remainCount) {
              remainCount.notifyAll();
            }
            return;
          }
          
          rowKey = iterator.next();
        }
        
        RowFilter rowFilter = new RowFilter(rowKey);
        for(CellFilter eachColumnFilter: cellFilters) {
          rowFilter.addCellFilter(eachColumnFilter);
        }
        try {
          Row row = ctable.get(rowFilter);
          dataReceiver.receive(rowKey, row);
        } catch (IOException e) {
          dataReceiver.error(rowKey, e);
        }
        synchronized(remainCount) {
          remainCount.decrementAndGet();
          remainCount.notifyAll();
          if(remainCount.get() == 0) {
            break;
          }
        }
      }
    }
  }    
}
