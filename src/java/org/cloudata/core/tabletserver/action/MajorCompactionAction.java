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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.fs.CloudataFileSystem;
import org.cloudata.core.tablet.TabletInfo;
import org.cloudata.core.tabletserver.Tablet;
import org.cloudata.core.tabletserver.action.TabletSplitAction.TabletSplitFinishAction;


public class MajorCompactionAction extends TabletAction {
  public static final Log LOG = LogFactory.getLog(MajorCompactionAction.class.getName());

  public static final String ACTION_TYPE = "MajorCompactionAction";
  
  static List<String> checkActionTypes = new ArrayList<String>();
  
  static List<String> checkWaitingActionTypes = new ArrayList<String>();
  
  static {  
    checkActionTypes.add(MinorCompactionAction.ACTION_TYPE);
    checkActionTypes.add(MajorCompactionAction.ACTION_TYPE);
    checkActionTypes.add(TabletSplitAction.ACTION_TYPE);
    checkActionTypes.add(TabletSplitFinishAction.ACTION_TYPE);    
    checkActionTypes.add(ScannerAction.ACTION_TYPE);
    checkActionTypes.add(BatchUploadAction.ACTION_TYPE);
    checkActionTypes.add(TabletLockAction.ACTION_TYPE);
    checkActionTypes.add(TabletDropAction.ACTION_TYPE);

    checkWaitingActionTypes.add(TabletSplitFinishAction.ACTION_TYPE);
  }
  
  private static ThreadLocal<MajorCompactContext> majorCompactContext 
    = new ThreadLocal<MajorCompactContext>();

  public static boolean isFastProcessingMode() {
    return majorCompactContext.get() == null || majorCompactContext.get().fastProcessing;
  }

  public static long getSlowProcessSleepTime() {
    return majorCompactContext.get().sleepTime;
  }
  
  static class MajorCompactContext {
    boolean fastProcessing;
    long sleepTime;
  }
  
  private Tablet tablet;
  private boolean end = true;
  private ActionFinalizer finalizer;
  
  public MajorCompactionAction(Tablet tablet) {
    this.tablet = tablet;
  }

  public boolean isThreadMode() {
    return true;
  }

  public boolean isWaitingMode(ActionChecker actionChecker) {
    return false;
  }
  
  private void setMajorCompactionPolicy() {
    if (majorCompactContext.get() == null) {
      majorCompactContext.set(new MajorCompactContext());
    }
    
    majorCompactContext.get().fastProcessing = !tablet.needRegulation();
    majorCompactContext.get().sleepTime = tablet.getConf().getInt("major.compact.regulation.sleeptime", 10);
  }

  public void run() {
    end = false;
    TabletInfo tabletInfo = null;
    long startTime = System.currentTimeMillis();
    try {
      if(tablet == null) {
        LOG.info("Can' start TabletMajorCompaction cause tablet is null");
        return;
      }
      tabletInfo = tablet.getTabletInfo();;
      setMajorCompactionPolicy();

      LOG.info("TabletMajorCompaction Start: " + tabletInfo + ", fastProcessing : " + isFastProcessingMode());
      
      if(!tablet.isReady()) {
        LOG.info("Can' start TabletMajorCompaction cause tablet not ready:" + tabletInfo);
        return;
      }
      
      tablet.getDiskSSTable().majorCompaction();
    } catch (Exception e) {
      LOG.error("MajorCompactionAction error:" + e.getMessage() + ", delete temp dir", e);
      CloudataFileSystem fs = CloudataFileSystem.get(tablet.getConf());
      try {
        fs.delete(Tablet.getTabletMajorCompactionTempPath(tablet.getConf(), tabletInfo), true);
      } catch (IOException err) {
        LOG.error("MajorCompactionAction error while delete temp:" + err.getMessage(), err);
      }
    } finally {
      if(tablet != null) {
        try {
          LOG.info("TabletMajorCompaction End: " + tabletInfo + ", time=" + (System.currentTimeMillis() - startTime) + 
              ", disk size=" + tablet.getDiskSSTable().sumMapFileSize());
          tablet.getTabletServer().tabletServerMetrics.setMajorCompactionTime(System.currentTimeMillis() - startTime);
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
        }
        tablet.getActionChecker().endAction(this);
      }
      end = true;
    }
  }
  
  public void setEnd(boolean end) {
    this.end = end;
  }
  public boolean isEnd() {
    return end;
  }

  @Override
  public String getActionKey() {
    return tablet == null ? "null:" + getActionType() : tablet.getTabletInfo().getTabletName() + getActionType();
  }

  public List<String> getRunCheckActionTypes() {
    return checkActionTypes;
  }
  
  public List<String> getWaitCheckActionTypes() {
    return checkWaitingActionTypes;
  }
  
  public String getActionType() {
    return ACTION_TYPE;
  }
}
