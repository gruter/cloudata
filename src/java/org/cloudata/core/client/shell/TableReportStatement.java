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

import java.io.IOException;
import java.util.StringTokenizer;

import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Shell;
import org.cloudata.core.client.cql.statement.ExecStatus;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.tablet.TabletInfo;
import org.cloudata.core.tablet.TabletReport;


/**
 * Table의 Tablet 정보를 Shell에 나타낸다.
 * @author jindolk
 *
 */
public class TableReportStatement extends ShellStatement {
  @Override
  public String getPrefix() {
    return StatementFactory.REPORT_TABLE;
  }

  @Override
  public ExecStatus execute(CloudataConf conf) {
    ExecStatus status = new ExecStatus();
    StringTokenizer st = new StringTokenizer(query);
    
    if(st.countTokens() < 3) {
      status.setMessage("Syntax error : Please check 'report table' syntax.");
      return status;
    }
    st.nextToken();
    st.nextToken();
    
    String tableName = st.nextToken();
    String tabletName = null;
    boolean prompt = false;
    if(st.hasMoreTokens()) {
      String token = st.nextToken();
      if(token.equals("prompt")) {
        prompt = true;
      } else {
        tabletName = token;
      }
    }
    
    if(st.hasMoreTokens()) {
      String token = st.nextToken();
      if(token.equals("prompt")) {
        prompt = true;
      }
    }
    
    if (tableName == null) {
      status.setMessage("Syntax error : Please check 'report table' syntax.");
      return status;
    }

    try {
      CTable ctable = CTable.openTable(Shell.conf, tableName);
      if(ctable == null) {
        throw new IOException("no table:" + tableName);
      }

      TabletInfo[] listTabletInfos = ctable.listTabletInfos();
      if(listTabletInfos == null)   return null;
      
      int count = 0;
      int errorCount = 0;
      long totalMapFileSize = 0;
      for(int i = 0; i < listTabletInfos.length; i++) {
        if(tabletName != null && !listTabletInfos[i].getTabletName().equals(tabletName)) {
          continue;
        }
        
        TabletReport tabletReport = ctable.getTabletReport(listTabletInfos[i]);
        if(tabletReport != null) {
          tabletReport.print();
          totalMapFileSize += tabletReport.sumMapFileSize();
          count++;
        } else {
          errorCount++;
          System.out.println("-----------------------------------------------------------------------------");
          System.out.println("Report Error:" + listTabletInfos[i]);
          System.out.println("-----------------------------------------------------------------------------");
        }
        
        if(prompt && (i + 1) % 10 == 0) {
          System.out.println("type any key to continue(q:quit)");
          byte b = (byte)System.in.read();
          if(b == 'q') {
            break;
          }
        }
      }
      System.out.println("Total MapFile size = " + totalMapFileSize + " bytes");
      status.setMessage("print out tablet info(success=" + count + " error=" + errorCount + " tablets).");
    } catch (IOException e) {
      status.setException(e);
    }
    
    return status;
  }

  @Override
  public HelpUsage getHelpUsage() {
    return new HelpUsage(getPrefix(), "report tablet status & tablet server status.", "REPORT TABLE <table_name> [tablet_name] [prompt]");
  }  
}
