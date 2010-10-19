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
import org.cloudata.core.common.util.StringUtils;


public class SetCharsetStatement extends ShellStatement {
  @Override
  public String getPrefix() {
    return StatementFactory.SET_CHARSET;
  }

  @Override
  public ExecStatus execute(CloudataConf conf) {
    ExecStatus status = new ExecStatus();
    String[] tokens = StringUtils.split(query, " ");
    if(tokens.length < 3) {
      status.setMessage("Syntax error : Please check 'set charset' syntax.");
      return status;
    }

    Shell.encoding = tokens[2];
    status.setMessage("changed charset to " + tokens[2]);
    
    return status;
  }

  @Override
  public HelpUsage getHelpUsage() {
    return new HelpUsage(getPrefix(), "Set Shell charset", "SET CHARSET <charset(utf-8|euc-kr|...)>");
  }
}
