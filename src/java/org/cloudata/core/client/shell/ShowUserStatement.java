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
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.cloudata.core.client.CTableManager;
import org.cloudata.core.client.TabletLocationCache;
import org.cloudata.core.client.cql.statement.ExecStatus;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.ipc.AclManager;
import org.cloudata.core.common.lock.LockUtil;


public class ShowUserStatement extends ShellStatement {
  @Override
  public String getPrefix() {
    return StatementFactory.SHOW_USER;
  }

  @Override
  public ExecStatus execute(CloudataConf conf) {
    ExecStatus status = new ExecStatus();
    try {
      ZooKeeper zk = TabletLocationCache.getInstance(conf).getZooKeeper();
      
      boolean isAdmin = AclManager.isSuperGroup(conf, zk, conf.getUserId());
      
      List<String> users = null;
      try {
        users = zk.getChildren(LockUtil.getZKPath(conf, org.cloudata.core.common.Constants.USERS), false);
      } catch(NoNodeException e) {
      } catch (Exception e) {
        throw new IOException(e);
      }
      
      int count = 0;
      if(users != null && users.size() > 0) {
        for(String eachUser: users) {
          if(isAdmin || !AclManager.isSuperGroup(conf, zk, eachUser)) {
            System.out.printf("%s\n", eachUser);
            count++;
          }
        }
      }      
      status.setMessage(count + " users found.");
    } catch (IOException e) {
      status.setException(e);
    }  
    return status;
  }

  @Override
  public HelpUsage getHelpUsage() {
    return new HelpUsage(getPrefix(), "list users.(only supergroup)", "SHOW USERS");
  }
}
