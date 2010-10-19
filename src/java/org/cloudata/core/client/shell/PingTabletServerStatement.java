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

import org.cloudata.core.client.Shell;
import org.cloudata.core.client.cql.statement.ExecStatus;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.ipc.CRPC;
import org.cloudata.core.common.util.NetworkUtil;
import org.cloudata.core.common.util.StringUtils;
import org.cloudata.core.tabletserver.DataServiceProtocol;


public class PingTabletServerStatement extends ShellStatement {
  @Override
  public String getPrefix() {
    return StatementFactory.PING_TABLETSERVER;
  }

  @Override
  public ExecStatus execute(CloudataConf conf) {
    ExecStatus status = new ExecStatus();
    String[] tokens = StringUtils.split(query, " ");
    if(tokens.length < 3) {
      status.setMessage("Syntax error : Please check 'ping tabletserver' syntax.");
      return status;
    }

    String hostName = tokens[2];
    int sleep = tokens.length > 3 ? Integer.parseInt(tokens[3]) : 0;
    String message = tokens.length > 4 ? tokens[4] : "Test";

    if(hostName.indexOf(":") < 0) {
      status.setMessage("Syntax error : host name must have port(host:port).");
      return status;
    }
    
    String resultMessage = null;
    try {
      DataServiceProtocol server = (DataServiceProtocol) CRPC.getProxy(DataServiceProtocol.class, 
          DataServiceProtocol.versionID, 
          NetworkUtil.getAddress(hostName), conf);       
      long startTime = System.currentTimeMillis();
      resultMessage = server.test(sleep * 1000, message);
      long endTime = System.currentTimeMillis();
      status.setMessage("success ping. return message= " + resultMessage + ", ping time=" + (endTime - startTime) + " ms");
    } catch (Exception e) {
      status.setException(e);
    }
    return status;
  }
  
  @Override
  public HelpUsage getHelpUsage() {
    return new HelpUsage(getPrefix(), "Pint to tabletserver", "PING TABLETSERVER <hostname:port> [sleep] [echo message]");
  }
}
