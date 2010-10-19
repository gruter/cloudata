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
package org.cloudata.core.tabletserver.action;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.common.util.SizeOf;
import org.cloudata.core.tabletserver.TxId;
import org.cloudata.core.tabletserver.action.TabletSplitAction.TabletSplitFinishAction;


/*
 * TabletSplitAction이 오랫동안 수행되고 있는 경우
 * Memory의 size가 일정 수준이상 커지게 되면 FlushAction이 lock이 걸린다.
 * 이 경우 Flush에 대한 lock이 걸려 전체 성능이 저하됨.
 */
public class FlushAction extends TabletAction {
  public static final Log LOG = LogFactory.getLog(FlushAction.class);
  public static final String ACTION_TYPE = "FlushAction";
  
  static List<String> checkActionTypes = new ArrayList<String>();
  static List<String> waitCheckActionTypes = new ArrayList<String>();
  
  static {
    checkActionTypes.add(TabletSplitFinishAction.ACTION_TYPE);
    checkActionTypes.add(TabletLockAction.ACTION_TYPE);
    
    waitCheckActionTypes.add(TabletSplitFinishAction.ACTION_TYPE);
    //waitCheckActionTypes.add(MinorCompactionAction.ACTION_TYPE);
  }

  private TxId txId;
  private int flushConcurrency;
  
  public FlushAction(TxId txId, int flushConcurrency) {
    this.txId = txId;
    this.flushConcurrency = flushConcurrency;
  }
  
  public String getActionKey() {
    return getActionType() + ":" + txId;
  }

  public String getActionType() {
    return ACTION_TYPE;
  }

  public List<String> getRunCheckActionTypes() {
    return checkActionTypes;
  }

  public List<String> getWaitCheckActionTypes() {
    return waitCheckActionTypes;
  }
  
  public boolean isThreadMode() {
    return false;
  }

  public boolean isWaitingMode(ActionChecker actionChecker) {
    return false;
  }

  public void run() {
    // do nothing
  }
  
  @Override
  public boolean canRun(ActionChecker actionChecker) {
    List<String> runCheckActions = getRunCheckActionTypes();
    
    if(runCheckActions != null) {
      for(String eachActionType: runCheckActions) {
        if(actionChecker.runningActions.contains(eachActionType)) {
          return false;
        }
      }
    }
    
    List<String> waitCheckActions = getWaitCheckActionTypes();
    if(waitCheckActions != null) {
      for(String eachActionType: waitCheckActions) {
        if(actionChecker.waitingActions.contains(eachActionType)) {
          return false;
        }
      }
    }

    if(SizeOf.getRealFreeMemory() < 100 * 1024 * 1024) {
      LOG.info("Low free memory:" + SizeOf.getRealFreeMemory());
      return false;
    }
    
    int flushActionCount = actionChecker.getRunningActionCount(ACTION_TYPE) ;
//    if(actionChecker.getRunningActionCount(TabletSplitAction.ACTION_TYPE) > 0) {
//      if(flushActionCount > 4) {
//        return false;
//      }
//    } 
    
    if(flushActionCount > flushConcurrency) {
      LOG.info("Can't start flush:too many flushActionCount:" + flushActionCount);
      return false;
    }

    return true;
  }  
}
