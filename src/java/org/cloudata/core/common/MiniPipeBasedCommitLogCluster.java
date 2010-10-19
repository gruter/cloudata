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

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.commitlog.CommitLogServer;
import org.cloudata.core.common.conf.CloudataConf;


public class MiniPipeBasedCommitLogCluster extends MiniCommitLogCluster {
  static final Log LOG = LogFactory.getLog(MiniPipeBasedCommitLogCluster.class);

  final CommitLogServer[] serverList = new CommitLogServer[3];

  public MiniPipeBasedCommitLogCluster(CloudataConf conf) {
    try {
      String rpcPortPropertyName = "cloudata.commitlog.server.port";
      String pipePortPropertyName = "commitLogServer.pipe.port";
      int rpcPort = conf.getInt(rpcPortPropertyName, 57001);
      int pipePort = conf.getInt(pipePortPropertyName, 18000);
      conf.set("commitLogServer.worker.count", 2);
      conf.set("commitLogServer.handler.count", 2);
      conf.set("cloudata.local.temp", System.getProperty("user.home")
          + File.separator + ".cloudata_local");

      serverList[0] = new CommitLogServer(conf);
      serverList[0].start();

      conf.setInt(rpcPortPropertyName, rpcPort + 1);
      conf.setInt(pipePortPropertyName, pipePort + 1);
      serverList[1] = new CommitLogServer(conf);
      serverList[1].start();

      conf.setInt(rpcPortPropertyName, rpcPort + 2);
      conf.setInt(pipePortPropertyName, pipePort + 2);
      serverList[2] = new CommitLogServer(conf);
      serverList[2].start();

      try {
        Thread.sleep(1000);
      } catch (Exception e) {
      }

      LOG
          .info("The mini cluster for pipe based Commitlog servers is ready... ");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void shutdown() throws IOException {
    Thread[] threadList = new Thread[serverList.length];

    for (int i = 0; i < threadList.length; i++) {
      final CommitLogServer server = serverList[i];
      threadList[i] = new Thread() {
        public void run() {
          if (server != null) {
            server.stop();
          }
        }
      };

      threadList[i].start();
    }

    try {
      Thread.sleep(2000);
    } catch (Exception e) {
    }

    for (int i = 0; i < threadList.length; i++) {
      try {
        threadList[i].join();
      } catch (InterruptedException e) {
      }
    }

    LOG.info("Mini pipe based commit log server cluster is successfully down");
  }
}