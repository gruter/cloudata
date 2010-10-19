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
package org.cloudata.core.client.shell;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.cloudata.core.client.CTableManager;
import org.cloudata.core.client.Shell;
import org.cloudata.core.client.cql.statement.ExecStatus;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.ipc.CRPC;
import org.cloudata.core.common.util.NetworkUtil;
import org.cloudata.core.master.TableManagerProtocol;
import org.cloudata.core.tabletserver.DataServiceProtocol;
import org.cloudata.core.tabletserver.TabletServerInfo;
import org.cloudata.core.tabletserver.action.BatchUploadAction;
import org.cloudata.core.tabletserver.action.MajorCompactionAction;
import org.cloudata.core.tabletserver.action.MinorCompactionAction;
import org.cloudata.core.tabletserver.action.ScannerAction;
import org.cloudata.core.tabletserver.action.TabletSplitAction;


public class StopActionStatement extends ShellStatement {
  @Override
  public String getPrefix() {
    return StatementFactory.STOP_TABLET_ACTION;
  }

  @Override
  public ExecStatus execute(CloudataConf conf) {
    ExecStatus status = new ExecStatus();
    List<String> params = new ArrayList<String>();
    
    byte[] queryBytes = query.getBytes();
    int pos = 0;
    int writeBytes = 0;
    
    ByteArrayOutputStream bout = new ByteArrayOutputStream(256);
    
    for(int i = 0; i < queryBytes.length; i++) {
      if(queryBytes[i] == ' ' || queryBytes[i] == '\n') {
        if(writeBytes > 0) {
          params.add(bout.toString());
          writeBytes = 0;
          bout.reset();
          pos++;
        }
      } else {
        bout.write(queryBytes[i]);
        writeBytes++;
      }
    }
    if(writeBytes > 0) {
      params.add(bout.toString());
      writeBytes = 0;
      bout.reset();
    }
    
    if(params.size() < 4) {
      status.setMessage("Syntax error : Please check 'stop' syntax.");
      return status;
    }
    String tableName = params.get(2);
    String actionName = params.get(3);
    
    String actionClassName = null;
    if("minor".equals(actionName)) {
      actionClassName = MinorCompactionAction.class.getName();
    } else if("major".equals(actionName)) {
      actionClassName = MajorCompactionAction.class.getName();
    } else if("split".equals(actionName)) {
      actionClassName = TabletSplitAction.class.getName();
    } else if("scan".equals(actionName)) {
      actionClassName = ScannerAction.class.getName();
    } else if("upload".equals(actionName)) {
      actionClassName = BatchUploadAction.class.getName();
    } else {
      status.setMessage("Syntax error : Please check 'stop' syntax.");
      return status;
    }
    
    if (tableName == null) {
      status.setMessage( "Syntax error : Please check 'stop' syntax.");
      return status;
    }

    String message = null;
    try {
      TableManagerProtocol masterServer = CTableManager.getMasterServer(Shell.conf);
      TabletServerInfo[] tabletServers = masterServer.getTabletServerInfos();
      for(TabletServerInfo eachTabletServerInfo: tabletServers) {
        try {
          DataServiceProtocol tabletServer = (DataServiceProtocol) CRPC.getProxy(DataServiceProtocol.class,
              DataServiceProtocol.versionID, NetworkUtil.getAddress(eachTabletServerInfo.getHostName()), Shell.conf);

          tabletServer.stopAction(tableName, actionClassName);
        } catch (IOException e) {
          message += eachTabletServerInfo.toString() + " error:" + e.getMessage() + "\n";
        }   
      }
      if(message != null) {
        status.setMessage("error msg : " + message);
      } else {
        status.setMessage("Action(" + actionName + ") stoped successfully.");
      }
    } catch (IOException e) {
      status.setException(e);
    }
    
    return status;
  }
  
  @Override
  public HelpUsage getHelpUsage() {
    return new HelpUsage(getPrefix(), "stop table action.", "STOP ACTION <table name> <action name(minor|major|split|scan|upload)>");
  }  
}
