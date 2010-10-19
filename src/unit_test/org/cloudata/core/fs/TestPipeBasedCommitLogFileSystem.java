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

import java.io.File;
import java.io.IOException;

import org.cloudata.core.commitlog.CommitLogClient;
import org.cloudata.core.common.MiniCommitLogCluster;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.fs.PipeBasedCommitLogFileSystem;
import org.cloudata.core.fs.PipeBasedCommitLogFileSystem.CommitLogFileSystemInfo;

import junit.framework.TestCase;


public class TestPipeBasedCommitLogFileSystem extends TestCase {
  MiniCommitLogCluster cluster;
  CloudataConf conf;
  protected void setUp() throws Exception {
    conf = new CloudataConf();
    conf.set("cloudata.commitlog.filesystem", "pipe");
    conf.set("cloudata.filesystem", "local");
    conf.set("cloudata.local.temp", System.getProperty("user.home") + File.separator + ".cloudata_local");

    cluster = MiniCommitLogCluster.create(conf);
  }

  protected void tearDown() throws Exception {
    cluster.shutdown();
  }
}
