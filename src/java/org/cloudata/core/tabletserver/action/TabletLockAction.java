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
import org.cloudata.core.tabletserver.action.TabletSplitAction.TabletSplitFinishAction;


public class TabletLockAction extends TabletAction {
  public static final Log LOG = LogFactory.getLog(TabletLockAction.class.getName());

  static List<String> checkActionTypes = new ArrayList<String>();
  static {  
    checkActionTypes.add(MinorCompactionAction.ACTION_TYPE);
    checkActionTypes.add(MajorCompactionAction.ACTION_TYPE);
    checkActionTypes.add(TabletSplitAction.ACTION_TYPE);
    checkActionTypes.add(TabletSplitFinishAction.ACTION_TYPE);
    checkActionTypes.add(CommitAction.ACTION_TYPE);
    checkActionTypes.add(ApplyAction.ACTION_TYPE);
    //checkActionTypes.add(TabletStopAction.ACTION_TYPE);
    checkActionTypes.add(ScannerAction.ACTION_TYPE);
    checkActionTypes.add(TxAction.ACTION_TYPE);
    checkActionTypes.add(BatchUploadAction.ACTION_TYPE);
  }

  public static final String ACTION_TYPE = "TabletLockAction";
  
  
  public TabletLockAction() {
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
    return getActionType();
  }

  @Override
  public String getActionType() {
    return ACTION_TYPE;
  }

  @Override
  public List<String> getRunCheckActionTypes() {
    return checkActionTypes;
  }
}
