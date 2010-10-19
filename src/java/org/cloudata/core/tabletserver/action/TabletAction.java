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

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class TabletAction implements ActionIF {
  public static final Log LOG = LogFactory.getLog(TabletAction.class.getName());

  private ActionFinalizer finalizer;
  
  public boolean equals(Object obj) {
    if(!(obj instanceof TabletAction)) {
      return false;
    }
    
    TabletAction otherAction = (TabletAction)obj;
    return getActionKey().equals(otherAction.getActionKey());
  }

  public List<String> getWaitCheckActionTypes() {
    return null;
  }
  
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
    
    return true;
  }
  
  public void setFinalizer(ActionFinalizer finalizer) {
    if (this.finalizer != null && finalizer != null) {
      this.finalizer.doFinalization();
      LOG.debug("Finalizer is overridden");
    }
    
    this.finalizer = finalizer;
  }
  
  public void finalizeAction() {
    if (finalizer != null) {
      finalizer.doFinalization();
      finalizer = null;
    }
  }
}
