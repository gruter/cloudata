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


public class ApplyAction extends TabletAction {
  public static final Log LOG = LogFactory.getLog(ApplyAction.class);
  public static final String ACTION_TYPE = "ApplyAction";
  
  static List<String> checkActionTypes = new ArrayList<String>();
  static List<String> waitCheckActionTypes = new ArrayList<String>();
  
  static {
//    checkActionTypes.addAll(TxAction.checkActionTypes);
//    checkActionTypes.addAll(CommitAction.checkActionTypes);
//    waitCheckActionTypes.addAll(TxAction.waitCheckActionTypes);

    checkActionTypes.add(TabletSplitFinishAction.ACTION_TYPE);
    checkActionTypes.add(TabletLockAction.ACTION_TYPE);
    
    waitCheckActionTypes.add(TabletSplitFinishAction.ACTION_TYPE);
  }

  TxId txId;
  String actionKey;
  
  public ApplyAction(TxId txId) {
    this.txId = txId;
    this.actionKey = getActionType() + ":" + txId;
  }
  
  public String getActionKey() {
    return actionKey;
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
}
