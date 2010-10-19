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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Latch {
  static Log LOG = LogFactory.getLog(Latch.class.getName());
  
  ReentrantLock latchLock = new ReentrantLock();
  boolean isBlocked = false;
  int numOfThread;
  Condition emptyCondition;
  Condition blockCondition;

  String tabletName;
  
  public Latch(String tabletName) {
    this.tabletName = tabletName;
    emptyCondition = latchLock.newCondition();
    blockCondition = latchLock.newCondition();
  }

  public boolean enter(long millisec, String txId) throws InterruptedException {
    if (millisec <= 0) {
      latchLock.lockInterruptibly();
    } else {
      if (!latchLock.tryLock(millisec, TimeUnit.MILLISECONDS)) {
        return false;
      }
    }
    
    try {
      while (isBlocked) {
        blockCondition.await();
      }
      numOfThread++;
      return true;
    } finally {
      latchLock.unlock();
    }
  }

  public void exit(String txId) {
    latchLock.lock();
    try {
      if (--numOfThread == 0 && isBlocked) {
        emptyCondition.signalAll();
      }
    } finally {
      latchLock.unlock();
    }
  }

  public void block() throws InterruptedException {
    latchLock.lockInterruptibly();
    try {
      isBlocked = true;
      while (numOfThread > 0) {
        emptyCondition.await();
      }
    } finally {
      latchLock.unlock();
    }
  }

  public void unblock() {
    latchLock.lock();
    try {
      isBlocked = false;
      blockCondition.signalAll();
    } finally {
      latchLock.unlock();
    }
  }
}
