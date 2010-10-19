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
package org.cloudata.core.common.metrics.system;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.cloudata.core.common.metrics.CloudataMetricsFactory;


/**
 * @author jindolk
 *
 */
public class NetParser implements SystemMetricsParser {
  private static final String ALL = "*";

  private Set<String> fields = new HashSet<String>();

  String fileName;
  
  @Override
  public void init(String fileName) throws IOException {
    this.fileName = fileName;
    
    String vmstatFields = CloudataMetricsFactory.getFactory().getAttribute("system." + fileName + ".field");

    if(vmstatFields != null) {
      if(ALL.equals(vmstatFields)) {
        fields.add("*");
      } else {
        String[] tokens = vmstatFields.split(",");
        
        for(String eachField: tokens) {
          fields.add(eachField);
        }
      }
    }
  }

  @Override
  public Map<String, Object> getMetricsValues() {
    boolean all = false;
    
    if(fields.size() == 0 || fields.contains(ALL)) {
      all = true;
    }

    BufferedReader reader = null;
    
    Map<String, Object> result = new HashMap<String, Object>();
    
    try {
      reader = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));
      
      while(true) {
        String nameLine = reader.readLine();
        if(nameLine == null) {
          return result;
        }
     
        String valueLine = reader.readLine();
        if(valueLine == null) {
          return result;
        }
  
        String[] attNameTokens = nameLine.split("[\\s]+"); 
        String[] valueTokens = valueLine.split("[\\s]+");
        
        if(attNameTokens.length != valueTokens.length) {
          CloudataMetricsFactory.LOG.error("mismatch # field name, # value");
          return result;
        }
        
        int index = 0;
        for(String eachAttName: attNameTokens) {
          if(all || fields.contains(eachAttName)) {
            result.put(eachAttName, valueTokens[index].trim());
          }
          index++;
        }
      }
    } catch (IOException e) {
      CloudataMetricsFactory.LOG.error("Can't read" + fileName + ":" + e.getMessage());
    } finally {
      if(reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
        }
      }
    }
    return result;
  }
  
//  public static void main(String[] args) {
//    String line1 = "TcpExt: SyncookiesSent SyncookiesRecv SyncookiesFailed EmbryonicRsts PruneCalled RcvPruned OfoPruned OutOfWindowIcmps LockDroppedIcmps ArpFilter TW TWRecycled TWKilled PAWSPassive PAWSActive PAWSEstab DelayedACKs DelayedACKLocked DelayedACKLost ListenOverflows ListenDrops TCPPrequeued TCPDirectCopyFromBacklog TCPDirectCopyFromPrequeue TCPPrequeueDropped TCPHPHits TCPHPHitsToUser TCPPureAcks TCPHPAcks TCPRenoRecovery TCPSackRecovery TCPSACKReneging TCPFACKReorder TCPSACKReorder TCPRenoReorder TCPTSReorder TCPFullUndo TCPPartialUndo TCPDSACKUndo TCPLossUndo TCPLoss TCPLostRetransmit TCPRenoFailures TCPSackFailures TCPLossFailures TCPFastRetrans TCPForwardRetrans TCPSlowStartRetrans TCPTimeouts TCPRenoRecoveryFail TCPSackRecoveryFail TCPSchedulerFailed TCPRcvCollapsed TCPDSACKOldSent TCPDSACKOfoSent TCPDSACKRecv TCPDSACKOfoRecv TCPAbortOnSyn TCPAbortOnData TCPAbortOnClose TCPAbortOnMemory TCPAbortOnTimeout TCPAbortOnLinger TCPAbortFailed TCPMemoryPressures";
//    String line2 = "TcpExt: 0 0 38657 349 0 0 0 0 0 0 279722 168 0 0 0 42 1675163 305 4187 0 0 12604140 2621486047 735444269 0 724342752 61880525 46815918 388269880 0 898 0 32 5719 0 4 4 20 33 3255 87374 40 1 492 6 91913 13300 9130 29174 0 58 0 0 4212 3 4933 5856 0 30419 16 0 60 0 0 0";
//
//    String[] tokens = line1.split("[\\s]+");
//    System.out.println("Length: " + tokens.length);
//    
//    for(String eachToken: tokens) {
//      System.out.println(">>>" + eachToken);
//    }
//  }
}
