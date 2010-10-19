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
package org.cloudata.core.fs;

import java.io.IOException;

import org.apache.zookeeper.ZooKeeper;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.tabletserver.TabletServer;


public abstract class CommitLogFileSystem implements CommitLogFileSystemIF {
  private static ThreadLocal<CommitLogFileSystemIF> threadLocal = new ThreadLocal<CommitLogFileSystemIF>();
  
  public static synchronized CommitLogFileSystemIF getCommitLogFileSystem(
                              CloudataConf conf,
                              TabletServer tabletServer,
                              ZooKeeper zk) throws IOException {
    CommitLogFileSystemIF fs = threadLocal.get();
    if(fs != null) {
      return fs;
    }
    
    String fileSystem = conf.get("cloudata.commitlog.filesystem", "local");
    if("local".equals(fileSystem)) {
      fs = new LocalCommitLogFileSystem(conf);
    } else if (fileSystem.indexOf("pipe") >= 0) {
        boolean consistencyCheck = "true".equalsIgnoreCase(
            conf.get("commitlog.pipe.consistency.check", "false"));
  	  fs = new PipeBasedCommitLogFileSystem(consistencyCheck);
  	  fs.init(fs, conf, tabletServer, zk);
    } else {
      //fs = new LocalCommitLogFileSystem();
      System.out.println("FATAL: can't get CommitLogFileSystem: " + fileSystem);
      System.exit(0);
    }
    threadLocal.set(fs);
    return fs;
  }
}
