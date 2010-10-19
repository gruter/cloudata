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
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.ZooKeeper;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.exception.TestCaseException;
import org.cloudata.core.common.lock.LockUtil;
import org.cloudata.core.tablet.TableSchema;
import org.cloudata.core.tablet.TabletInfo;
import org.cloudata.core.tabletserver.TabletServer;
import org.cloudata.core.tabletserver.action.TabletSplitAction.TabletSplitFinishAction;


public class TabletDropAction extends TabletAction {
  public static final Log LOG = LogFactory.getLog(TabletDropAction.class.getName());
  public static final String ACTION_TYPE = "TabletDropAction";
  
  static List<String> checkActions = new ArrayList<String>();
  static {  
    checkActions.add(TabletSplitAction.ACTION_TYPE);
    checkActions.add(TabletSplitFinishAction.ACTION_TYPE);
    checkActions.add(MinorCompactionAction.ACTION_TYPE);
    checkActions.add(MajorCompactionAction.ACTION_TYPE);
  }
  
  private TabletInfo tabletInfo;
  private ZooKeeper zk;
  protected TabletServer tabletServer;
  
  public TabletDropAction() {
  }
  
  public void init(TabletServer tabletServer, ZooKeeper zk, TabletInfo tabletInfo) {
    this.tabletServer = tabletServer;
    this.zk = zk;
    this.tabletInfo = tabletInfo;  
  }
  
  public TabletServer getTabletServer() {
    return tabletServer;
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
    return checkActions;
  }

  @Override
  public boolean isThreadMode() {
    return true;
  }

  @Override
  public boolean isWaitingMode(ActionChecker actionChecker) {
    return true;
  }

  public void run() {
    try {
      LockUtil.delete(zk, LockUtil.getZKPath(tabletServer.getConf(), 
          Constants.UPLOADER + "/" + tabletInfo.getTabletName()), true);
    } catch (Exception e) {
      LOG.error("Can't delete UPLOADER lock," + tabletInfo.getTabletName(), e);
    }
  }
  
  public String getTestHandlerKey() {
    return tabletServer.getHostName();
  }
  
//  public static void main(String[] args) throws IOException {
//    DecimalFormat df = new DecimalFormat();
//    Row.Key[] rowKeys = new Row.Key[100];
//    for(int i = 0; i < 100; i++) {
//      rowKeys[i] = new Row.Key(df.format(i));
//    }
//    
//    rowKeys[99] = Row.Key.MAX_KEY;
//    
//    NConfiguration conf = new NConfiguration(); 
//    TableSchema tableSchema = new TableSchema(args[0], "Test", new String[]{"Col1", "Col2"});
//    NTable.createTable(conf, tableSchema, rowKeys);
//  }
}
