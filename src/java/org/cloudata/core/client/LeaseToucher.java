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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.tablet.TabletInfo;
import org.cloudata.core.tabletserver.DataServiceProtocol;


/**
 * @author jindolk
 *
 */
public class LeaseToucher implements Runnable {
  private static final Log LOG = LogFactory.getLog(LeaseToucher.class.getName());
  String leaseId;
  CloudataConf conf;
  String hostName;
  TabletInfo tabletInfo;
  
  public LeaseToucher(
          TabletInfo tabletInfo,
          CloudataConf conf,
          String hostName, 
          String leaseId) {
    this.leaseId = leaseId; 
    this.conf = conf;
    this.hostName = hostName;
    this.tabletInfo = tabletInfo;
  }
  
  public void run() {
    try {
      try {
        Thread.sleep(5 * 1000);
      } catch (InterruptedException e) {
        return;
      }
      int retry = 0;
      DataServiceProtocol tabletServer = null;
      while(true) {
        try {
          tabletServer = CTableManager.connectTabletServer(hostName, conf);
          break;
        } catch (Exception e) {
          retry++;
          if(retry >= 5) {
            LOG.error("Touch Open Error:Can't connectTabletServer:" + hostName + "," + tabletInfo , e);
          }
          try {
            Thread.sleep(3 * 1000);
          } catch (InterruptedException err) {
            return;
          }
        }
      }
      
      while(true) {
        try {
          tabletServer.touch(leaseId);
        } catch (Exception e) {
          LOG.error("Scanner Touch Error:" + hostName + "," + e.getMessage());
        }
        try {
          Thread.sleep(10 * 1000);
        } catch (InterruptedException e) {
          return;
        }
      }
    } catch (Exception e) {
      LOG.error("LeaseTouch Open Error:" + hostName + "," + tabletInfo , e);
    }
  }

}
