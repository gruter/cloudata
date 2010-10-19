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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Lease {
  public static final Log LOG = LogFactory.getLog(Lease.class.getName());
  private Object leaseId;
  private LeaseListener leaseListener;
  private int validPeriod;
  private long lastUpdate;
  
  public Lease(Object leaseId, LeaseListener leaseListener, int validPeriod) {
    this.leaseId = leaseId;
    this.leaseListener = leaseListener;
    this.validPeriod = validPeriod;
    this.lastUpdate = System.currentTimeMillis();
  }

  public Object getLeaseId() {
    return leaseId;
  }
  
  public void touch() {
    lastUpdate = System.currentTimeMillis();
  }
  
  public void expire() {
    if(leaseListener != null) {
      leaseListener.expire();
    }
  }
  
  public boolean isExipred() {
    long currentTime = System.currentTimeMillis();
    return (currentTime - lastUpdate) > validPeriod;
  }
}
