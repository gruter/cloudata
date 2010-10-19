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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.common.conf.CloudataConf;


public abstract class MiniCommitLogCluster {
  static final Log LOG = LogFactory.getLog(MiniCommitLogCluster.class);

  public static MiniCommitLogCluster create(CloudataConf conf) throws IOException {
    String fsType = conf.get("cloudata.commitlog.filesystem", "pipe");
    
    if (fsType.equals("file")) {
      return new MiniFileBasedCommitLogCluster(conf);
    } else if (fsType.equals("pipe")) {
      return new MiniPipeBasedCommitLogCluster(conf);
    }

    throw new IOException("Unsupported commit log file system, fsType : " + fsType);
  }

  public abstract void shutdown()  throws IOException;
}
