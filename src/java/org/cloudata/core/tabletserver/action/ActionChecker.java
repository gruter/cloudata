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
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.common.MutableInteger;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.tablet.TabletInfo;
import org.cloudata.core.tabletserver.action.TabletSplitAction.TabletSplitFinishAction;


public class ActionChecker {
  public static final Log LOG = LogFactory.getLog(ActionChecker.class.getName());

  protected Actions runningActions = new Actions();

  protected ReentrantLock waitingLock = new ReentrantLock();
  protected Actions waitingActions = new Actions();

  protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  protected ActionChecker actionChecker;

  protected TabletInfo tabletInfo;

  protected ThreadGroup threadGroup;

  private ThreadPoolExecutor compactionExecutor;

  private ThreadPoolExecutor splitExecutor;

  private ThreadPoolExecutor actionExecutor;

  ExecutorService waitActionStarter = Executors.newSingleThreadExecutor();

  private CloudataConf conf;

  private int maxMajorCompactionThread;

  private int maxSplitThread;

  // FIXME Sync에 대해 정리 필요, 가끔 MajorCompaction과 MinorCompaction이 같이 수행되는 경우가 발생하여
  // 현재는 모두 sync 처리 했음 -> 성능 확인 및 적절하게 lock 구성 필요
  public ActionChecker(CloudataConf conf, ThreadPoolExecutor compactionExecutor,
      ThreadPoolExecutor splitExecutor, ThreadPoolExecutor actionExecutor, ThreadGroup threadGroup,
      TabletInfo tabletInfo) {
    this.conf = conf;
    this.actionChecker = this;
    this.threadGroup = threadGroup;
    this.tabletInfo = tabletInfo;
    this.splitExecutor = splitExecutor;
    this.compactionExecutor = compactionExecutor;
    this.actionExecutor = actionExecutor;

    this.maxMajorCompactionThread = conf.getInt("tabletServer.maxMajorCompactionThread", 3);
    this.maxSplitThread = conf.getInt("tabletServer.maxSplitThread", 3);
  }

  public Collection<String> getRunningActions() {
    lock.readLock().lock();
    try {
      return runningActions.getListForReport();
    } finally {
      lock.readLock().unlock();
    }
  }

  public Collection<String> getWaitingActions() {
    lock.readLock().lock();
    try {
      return waitingActions.getListForReport();
    } finally {
      lock.readLock().unlock();
    }
  }

  private boolean startAction(ActionIF action, boolean addWaitingList) {
    boolean runFinalizer = false;
    boolean spawnThread = false;

    //NStopWatch watch = new NStopWatch();
    //watch.start("ActionChecker.startAction lock.writeLock", 10);
    lock.writeLock().lock();
    //watch.stopAndReportIfExceed(LOG);
    
    //watch.start("ActionChecker.startAction run", 10);
    try {
      boolean canRun = action.canRun(this);
      if (canRun) {
        if (action.isThreadMode()) {
          spawnThread = true;
        }

        runningActions.add(action);
      } else {
        if (addWaitingList && action.isWaitingMode(this) && !internalHasAlreadyScheduled(action)) {
          waitingActions.add(action);
        } else {
          runFinalizer = true;
        }
      }
      return canRun;
    } finally {
      //watch.stopAndReportIfExceed(LOG);
      lock.writeLock().unlock();

      if (spawnThread) {
        startActionThread(action);
      }
      
      if (runFinalizer) {
        action.finalizeAction();
      }
    }
  }

  public boolean hasAlreadyScheduled(ActionIF action) {
    lock.readLock().lock();
    try {
      return internalHasAlreadyScheduled(action);
    } finally {
      lock.readLock().unlock();
    }
  }
  
  private boolean internalHasAlreadyScheduled(ActionIF action) {
    return runningActions.contains(action.getActionType())
    || waitingActions.contains(action.getActionType());
  }

  public boolean startAction(ActionIF action) {
    return startAction(action, true);
  }

  public void endAction(TabletAction action) {
    if(action == null) {
      return;
    }
    action.finalizeAction();

    boolean promote = false;
    lock.writeLock().lock();
    try {
      runningActions.remove(action);
      promote = !waitingActions.actionSet.isEmpty();
    } finally {
      lock.writeLock().unlock();
    }
    
    if (promote) {
      promoteWaitingActionToRunnning();
    }
  }

  // by sangchul
  // This method is rarely called
  // Little possibility for this method to impact overall performance of the system
  public void endActionType(TabletAction action) {
    action.finalizeAction();

    lock.writeLock().lock();
    try {
      runningActions.removeType(action);
      waitingActions.removeType(action);
    } finally {
      lock.writeLock().unlock();
    }
    promoteWaitingActionToRunnning();
  }
  
  private void promoteWaitingActionToRunnning() {
    if (waitingLock.tryLock()) {
      try {
        waitActionStarter.execute(new Runnable() {
          public void run() {
            lock.writeLock().lock();
            try {
              waitingActions.startWaitAction();
            } catch(Throwable e) {
              LOG.error("Error in starting wait action", e);
            } finally {
              lock.writeLock().unlock();
            }
          }
        });
      } finally {
        waitingLock.unlock();
      }
    }
  }

  private void startActionThread(ActionIF action) {
    if (MajorCompactionAction.ACTION_TYPE.equals(action.getActionType())) {
      compactionExecutor.execute(action);
//    } else if (TabletSplitAction.ACTION_TYPE.equals(action.getActionType()) ||
//        TabletSplitFinishAction.ACTION_TYPE.equals(action.getActionType())) {
//      splitExecutor.execute(action);
    } else if (TabletSplitAction.ACTION_TYPE.equals(action.getActionType())) {
      splitExecutor.execute(action);
    } else if (TabletSplitFinishAction.ACTION_TYPE.equals(action.getActionType())) {
      Thread t = new Thread(threadGroup, action);
      t.start();
    } else {
      actionExecutor.execute(action);
    }
  }

  public void endActions() {
    lock.writeLock().lock();
    try {
      runningActions.clear();
      waitingActions.clear();
    } finally {
      lock.writeLock().unlock();
    }
  }

  public boolean hasAction(String actionKey) {
    lock.readLock().lock();
    try {
      return runningActions.actionMap.containsKey(actionKey)
          || waitingActions.actionMap.containsKey(actionKey);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void print() {
    lock.readLock().lock();
    try {
      LOG.debug("=====Running Actions=====");
      runningActions.print();
      LOG.debug("=====Waiting Actions=====");
      waitingActions.print();
    } finally {
      lock.readLock().unlock();
    }
  }

  public List<String> getAllActions() {
    List<String> actionNames = new ArrayList<String>();
    actionNames.add("[Running Actions]");
    
    lock.readLock().lock();
    try {
      actionNames.addAll(runningActions.getListForReport());
      actionNames.add("[Waiting Actions]");
      actionNames.addAll(waitingActions.getListForReport());
    } finally {
      lock.readLock().unlock();
    }
    
    return actionNames;
  }

  public int getRunningActionCount(String actionType) {
    lock.readLock().lock();
      try {
      MutableInteger value = runningActions.actionTypeList.get(actionType);
  
      if (value != null) {
        return value.getValue();
      } else {
        return 0;
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  class Actions {
    // action key
    TreeSet<String> actionSet = new TreeSet<String>();

    // action key
    Map<String, ActionInfo> actionMap = new HashMap<String, ActionInfo>();

    // action type
    Map<String, MutableInteger> actionTypeList = new HashMap<String, MutableInteger>();

    public void add(ActionIF action) {
      actionSet.add(action.getActionKey());
      actionMap.put(action.getActionKey(), new ActionInfo(action));

      MutableInteger count = actionTypeList.get(action.getActionType());
      if (count == null) {
        count = new MutableInteger(0);
        actionTypeList.put(action.getActionType(), count);
      }
      count.add();
    }

    public int getActionCount(String actionKey) {
      MutableInteger count = actionTypeList.get(actionKey);
      if (count == null)
        return 0;
      return count.getValue();
    }

    public void print() {
      String tabletName = "";
      if (tabletInfo != null)
        tabletName = tabletInfo.getTabletName();
      for (String eachAction : actionSet) {
        ActionInfo actionInfo = actionMap.get(eachAction);
        LOG.debug(tabletName + ":" + actionInfo.toString());
      }
    }

    public Collection<String> getListForReport() {
      List<String> result = new ArrayList<String>();
      for (String eachAction : actionSet) {
        ActionInfo actionInfo = actionMap.get(eachAction);
        if (actionInfo != null) {
          result.add(actionInfo.getTabletAction().getActionKey() + ":"
              + new Date(actionInfo.getTime()));
        }
      }
      return result;
    }

    public void startWaitAction() {
      for (String eachActionKey : actionSet) {
        ActionInfo actionInfo = actionMap.get(eachActionKey);
        ActionIF action = actionInfo.getTabletAction();
        if (action.canRun(actionChecker) && action.isThreadMode()) {
          if (startAction(action, false)) {
            remove(action);
          }
        }
      }
    }

    public void clear() {
      actionSet.clear();
      actionMap.clear();
      actionTypeList.clear();
    }

    public void remove(ActionIF action) {
      String actionKey = action.getActionKey();
      actionSet.remove(actionKey);
      actionMap.remove(actionKey);

      MutableInteger count = actionTypeList.get(action.getActionType());
      if (count != null) {
        count.minus();
        if (count.getValue() == 0) {
          actionTypeList.remove(action.getActionType());
        }
      }
    }

    public void removeType(ActionIF action) {
      for (Map.Entry<String, ActionInfo> entry : actionMap.entrySet()) {
        ActionInfo actionInfo = entry.getValue();
        if (actionInfo.getTabletAction().getClass().getName().equals(action.getClass().getName())) {
          actionSet.remove(entry.getKey());
          actionMap.remove(entry.getKey());
        }
      }

      MutableInteger count = actionTypeList.get(action.getActionType());
      if (count != null) {
        actionTypeList.remove(action.getActionType());
      }
    }

    public boolean contains(String actionType) {
      return actionTypeList.containsKey(actionType);
    }
  }

  class ActionInfo {
    ActionIF tabletAction;
    long time;

    public ActionInfo(ActionIF tabletAction) {
      this.tabletAction = tabletAction;
      this.time = System.currentTimeMillis();
    }

    public ActionIF getTabletAction() {
      return tabletAction;
    }

    public long getTime() {
      return time;
    }

    public String toString() {
      return tabletAction.getActionKey() + ":" + new Date(time);
    }
  }
}
