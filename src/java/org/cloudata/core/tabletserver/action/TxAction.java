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
import org.cloudata.core.tabletserver.TxId;
import org.cloudata.core.tabletserver.action.TabletSplitAction.TabletSplitFinishAction;


public class TxAction extends TabletAction {
  public static final Log LOG = LogFactory.getLog(TxAction.class.getName());
  public static final String ACTION_TYPE = "TxAction";
  
  static List<String> checkActionTypes = new ArrayList<String>();
  static List<String> waitCheckActionTypes = new ArrayList<String>();
  
  static {  
    //checkActionTypes.add(TabletSplitAction.ACTION_TYPE);
    checkActionTypes.add(TabletSplitFinishAction.ACTION_TYPE);
    //checkActionTypes.add(TabletStopAction.ACTION_TYPE);
    checkActionTypes.add(TabletLockAction.ACTION_TYPE);
    
    waitCheckActionTypes.add(TabletSplitFinishAction.ACTION_TYPE);
  }
  
  private TxId txId;
  
  public TxAction(TxId txId) {
    this.txId = txId;
  }
  public boolean isThreadMode() {
    return false;
  }

  public boolean isWaitingMode(ActionChecker actionChecker) {
    return false;
  }

  public void run() {
    
  }

  @Override
  public String getActionKey() {
    return getActionType() + ":" + txId.toString();
  }
  
  @Override
  public String getActionType() {
    return ACTION_TYPE;
  }
  
  @Override
  public List<String> getRunCheckActionTypes() {
    return checkActionTypes;
  }
  
  public List<String> getWaitCheckActionTypes() {
    //SplitFinish 작업이 대기 중이면 Tx를 시작하지 않는다. 이것은 SplitFinish 작업을 시작해야 하는데
    //계속해서 Tx 요총이 들어오면 처리할 SplitFinish 작업이 계속 wait 되기 때문이다.
    return waitCheckActionTypes;
  }

}
