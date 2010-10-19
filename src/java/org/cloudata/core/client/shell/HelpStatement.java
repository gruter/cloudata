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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

import org.cloudata.core.client.cql.statement.ExecStatus;
import org.cloudata.core.client.cql.statement.QueryStatement;
import org.cloudata.core.common.conf.CloudataConf;



public class HelpStatement extends ShellStatement {
  @Override
  public ExecStatus execute(CloudataConf conf) {
    if(query == null || query.length() == 0) {
      printAllUsages();
      return null;
    }
    StringTokenizer st = new StringTokenizer(query);
    if(st.countTokens() == 0) {
      printAllUsages();
      return null;
    }
    st.nextToken();
    
    String helpTargetQuery = "";
    while(st.hasMoreTokens()) {
      helpTargetQuery += st.nextToken() + " ";
    }
    
    try {
      if(helpTargetQuery == null || helpTargetQuery.trim().length() == 0) {
        printAllUsages();
        return null;
      }
      
      QueryStatement stmt = StatementFactory.getStatement(helpTargetQuery.trim());
      if(stmt == null || stmt instanceof HelpStatement) {
        System.out.println("\n");
        printAllUsages();
      } else {
        HelpUsage helpUsage = StatementFactory.getUsages().get(stmt.getPrefix());
        print(helpUsage, true);
      }
    } catch (Exception e) {
      ExecStatus status = new ExecStatus(e);
      return status;
    }
    return null;
  }

  private void printAllUsages() {
    Collection<HelpUsage> usages = StatementFactory.getUsages().values();
    List<HelpUsage> usageList = new ArrayList<HelpUsage>();
    usageList.addAll(usages);
    Collections.sort(usageList);
    for(HelpUsage eachUsage: usageList) {
      print(eachUsage, false);
    }
  }
  
  @Override
  public String getPrefix() {
    return StatementFactory.HELP;
  }
  
  public static void print(HelpUsage helpUsage, boolean example) {
    System.out.printf("%-30s", " " + helpUsage.getCommand());
    if (helpUsage.getDesc().length() > 65) {
      System.out.println(helpUsage.getDesc().substring(0, 65));
    } else {
      System.out.println(helpUsage.getDesc());
    }

    if (example)
      System.out.println("\n Usage: \n" + helpUsage.getUsage() + "\n");
  }

  @Override
  public HelpUsage getHelpUsage() {
    return new HelpUsage(getPrefix(), "Help", "HELP");
  }
}
