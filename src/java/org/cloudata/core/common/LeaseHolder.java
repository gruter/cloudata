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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LeaseHolder {
  public static final Log LOG = LogFactory.getLog(LeaseHolder.class.getName());
  
  private Map<Object, Lease> leaseMap = new HashMap<Object, Lease>();
  
  public LeaseHolder() {
    this(null);
  }
  
  public LeaseHolder(ThreadGroup threadGroup) {
    Thread leaseMonitorThread = null;
    if(threadGroup != null) {
      leaseMonitorThread = new Thread(threadGroup, new LeaseMonitor());
    } else {
      leaseMonitorThread = new Thread(new LeaseMonitor());
    }
    leaseMonitorThread.start();
  }
  public void createLease(Object leaseId, LeaseListener leaseListener, int validPeriod) {
    synchronized(leaseMap) {
      if(leaseMap.containsKey(leaseId)) {
        touch(leaseId);
      } else {
        leaseMap.put(leaseId, new Lease(leaseId, leaseListener, validPeriod));
      }
    }
    //LOG.debug("createLease: totalLeaseCount=" + leaseMap.size());
  }
  
  public void touch(Object leaseId) {
    synchronized(leaseMap) {
      Lease lease = leaseMap.get(leaseId);
      if(lease != null) {
        lease.touch();
      }
    }
  }
  
  public void removeLease(Object leaseId) {
    synchronized(leaseMap) {
      leaseMap.remove(leaseId);
    }
    //LOG.debug("removeLease: totalLeaseCount=" + leaseMap.size());
  }
  
  public boolean containsLease(Object leaseId) {
    synchronized(leaseMap) {
      return leaseMap.containsKey(leaseId);
    }
  }
  
  class LeaseMonitor implements Runnable {
    public void run() {
      Map<Object, Lease> copyedLeaseMap = new HashMap<Object, Lease>();
      while(true) {
        synchronized(leaseMap) {
          copyedLeaseMap.putAll(leaseMap);
        }
        
        for(Lease lease: copyedLeaseMap.values()) {
          if(lease.isExipred()) {
            lease.expire();
            synchronized(leaseMap) {
              leaseMap.remove(lease.getLeaseId());
            }
          }
        }
        copyedLeaseMap.clear();
        try {
          Thread.sleep(5 * 1000);
        } catch (InterruptedException e) {
          return;
        }
      }
    }
  }
}
