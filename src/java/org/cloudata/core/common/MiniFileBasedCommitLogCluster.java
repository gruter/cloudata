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
package org.cloudata.core.common;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.cloudata.core.commitlog.CommitLogServer;
import org.cloudata.core.commitlog.CommitLogServerIF;
import org.cloudata.core.common.conf.CloudataConf;


public class MiniFileBasedCommitLogCluster extends MiniCommitLogCluster {
  protected Map<String, CommitLogServerIF> commitLogServers;

  protected CloudataConf conf;

  public MiniFileBasedCommitLogCluster(CloudataConf conf) throws IOException {
    this.conf = conf;
    this.commitLogServers = new HashMap<String, CommitLogServerIF>();

    CommitLogServer.formatAll();
    startCommitLogServers();
  }

  protected void startCommitLogServers() throws IOException {
    for (int i = 0; i < 3; i++) {
      CloudataConf commitLogServerConf = new CloudataConf(conf);
      commitLogServerConf.set("cloudata.commitlog.server.port",
          commitLogServerConf.getInt("cloudata.commitlog.server.port", 57001)
              + i);
      commitLogServerConf.set("commitlog.data.dir", commitLogServerConf
          .get("commitlog.data.dir")
          + "/" + (i + 1));

      CommitLogServerIF commitLogServer = CommitLogServer.create(conf);
      this.commitLogServers.put(commitLogServer.getHostInfo(), commitLogServer);
      commitLogServer.start();
    }
  }

  public void shutdown() throws IOException {
    for (Map.Entry<String, CommitLogServerIF> entry : commitLogServers
        .entrySet()) {
      entry.getValue().stopCommitLogServer();
    }
  }
}
