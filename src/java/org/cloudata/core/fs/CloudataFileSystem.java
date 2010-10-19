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

/**
 * Cloudata의 데이터파일을 저장하는 파일시스템에 대한 interface
 * Cloudata은 특정 파일시스템에 종속되지 않으며 분산된 환경에서 
 * 파일의 URI에 대해 Global namespace를 제공하는 모든 유형의 파일시스템에 적용 가능하다.
 * Cloudata을 특정 파일시스템에 포팅시키기 위해서는 이 클래스를 상속받아 메소드를 포팅할
 * 파일시스템에 맞게 적절하게 구현하면 된다.
 */
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.common.conf.CloudataConf;


public abstract class CloudataFileSystem {
  protected CloudataConf nconf;
  
  public static final Log LOG = LogFactory.getLog(CloudataFileSystem.class.getName());
  private static ReentrantReadWriteLock fileSystemsLock = new ReentrantReadWriteLock();
  private static Map<String, CloudataFileSystem> fileSystems = new HashMap<String, CloudataFileSystem>();

  protected CloudataFileSystem(CloudataConf nconf) {
    this.nconf = nconf;
  }
  
  public static CloudataFileSystem get(CloudataConf conf, String fsName) {
    if (fsName == null) {
      LOG.fatal("fs name is null");
      return null;
    }
    if (conf.get("cloudata.local.temp") == null) {
      LOG.fatal("no cloudata.local.temp property value. check cloudata-site.xml");
      return null;
    }
    
    CloudataFileSystem fs = null;
    fileSystemsLock.readLock().lock();
    try {
      if ((fs = fileSystems.get(fsName)) != null) {
        return fs;
      }
    } finally {
      fileSystemsLock.readLock().unlock();
    }
    
    fileSystemsLock.writeLock().lock();
    try {
      if ((fs = fileSystems.get(fsName)) != null) {
        return fs;
      }

      if ("local".equals(fsName)) {
        fs = new LocalFileSystem(conf);
      } else if ("hadoop".equals(fsName)) {
        fs = new HadoopFileSystem(conf);
      } else {
        LOG.error("Invalid file system name [" + fsName + "]");
        return null;
      }
      fileSystems.put(fsName, fs);
    } catch(IOException e) {
      LOG.fatal("Fail to initialize file system", e);
    } finally {
      fileSystemsLock.writeLock().unlock();
    }

    return fs;
  }

  public static CloudataFileSystem get(CloudataConf conf) {
    String fileSystem = conf.get("cloudata.filesystem", "local");
    return CloudataFileSystem.get(conf, fileSystem);
  }

  public boolean isReady() {
    try {
      exists(new GPath(nconf.get("cloudata.root")));
      return true;
    } catch (Exception e) {
      LOG.error("Ready check error:" + e.getMessage(), e);
      return false;
    }
  }
  
  public abstract void setPermission(GPath path, short mode) throws IOException;

  public abstract boolean renameTo(GPath targetPath, GPath destPath) throws IOException;

  public abstract boolean exists(GPath path) throws IOException;

  public abstract boolean mkdirs(GPath path) throws IOException;

  public abstract GPath[] list(GPath path) throws IOException;

  public abstract OutputStream create(GPath path) throws IOException;

  public abstract OutputStream create(GPath path, short replication) throws IOException;
  
  public abstract OutputStream create(GPath path, int bufferSize) throws IOException;

  public abstract OutputStream create(GPath path, int bufferSize, short replication) throws IOException;

  public abstract InputStream open(GPath path) throws IOException;

  public abstract InputStream open(GPath path, int bufferSize) throws IOException;

  public abstract void seek(InputStream in, long offset) throws IOException;

  public abstract boolean delete(GPath path) throws IOException;

  public abstract boolean move(GPath targetPath, GPath destPath) throws IOException;

  public abstract boolean isDirectory(GPath path) throws IOException;

  public abstract long getLength(GPath path) throws IOException;
  
  public abstract DataInputStream openDataInputStream(GPath path) throws IOException;

  public abstract DataInputStream openDataInputStream(GPath path, int bufferSize) throws IOException;

  public abstract void setReplication(GPath path, short replication) throws IOException;
  // public abstract String[][] getFileCacheHints(GPath path) throws
  // IOException;

  // public abstract LocatedBlocks getBlockInfos(GPath path) throws IOException;
  //  
  // public abstract DataInputStream openDataInputStream(GPath path,
  // LocatedBlocks dfsBlockInfos) throws IOException;

  public abstract void close() throws IOException;

  public abstract boolean delete(GPath path, boolean recursive) throws IOException;
}
