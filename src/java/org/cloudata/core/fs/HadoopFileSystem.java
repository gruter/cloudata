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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.cloudata.core.common.conf.CloudataConf;


/**
 * cloudata - HDFS 구성을 지원하는 파일시스템
 * 
 * @author babokim
 * 
 */
public class HadoopFileSystem extends CloudataFileSystem {
  public static final Log LOG = LogFactory.getLog(HadoopFileSystem.class.getName());
  
  FileSystem fs;
  
  protected HadoopFileSystem(CloudataConf cconf) throws IOException {
    super(cconf);
    Configuration conf = new Configuration();

    if (cconf.get("filesystem.url") != null) {
      conf.set("fs.default.name", cconf.get("filesystem.url"));
    }
    fs = FileSystem.get(conf);
  }

  public boolean exists(GPath path) throws IOException {
    return fs.exists(new Path(path.toString()));
  }

  public boolean mkdirs(GPath path) throws IOException {
    Path hPath = new Path(path.toString());
    boolean result =  fs.mkdirs(hPath);
    
    return result;
  }

  public GPath[] list(GPath path) throws IOException {
    FileStatus[] paths = fs.listStatus(new Path(path.toString()));
    if (paths == null || paths.length == 0) {
      return null;
    }

    GPath[] result = new GPath[paths.length];
    for (int i = 0; i < paths.length; i++) {
      result[i] = new GPath(paths[i].getPath().toString());
    }
    return result;
  }

  public OutputStream create(GPath path) throws IOException {
    return fs.create(new Path(path.toString()));
  }

  public OutputStream create(GPath path, short replication) throws IOException {
    return fs.create(new Path(path.toString()), replication);
  }
  
  public OutputStream create(GPath path, int bufferSize) throws IOException {
    return fs.create(new Path(path.toString()), true, bufferSize);
  }
  
  public OutputStream create(GPath path, int bufferSize, short replication) throws IOException {
    return fs.create(new Path(path.toString()), true, bufferSize, replication, fs.getDefaultBlockSize());
  }

  public void setReplication(GPath path, short replication) throws IOException {
    fs.setReplication(new Path(path.toString()), replication);
  }
  
  public InputStream open(GPath path) throws IOException {
    return fs.open(new Path(path.toString()));
  }

  public InputStream open(GPath path, int bufferSize) throws IOException {
    return fs.open(new Path(path.toString()), bufferSize);
  }

  public void seek(InputStream in, long offset) throws IOException {
    ((FSDataInputStream) in).seek(offset);
  }

  public boolean delete(GPath path) throws IOException {
    return fs.delete(new Path(path.toString()), false);
  }

  public boolean move(GPath targetPath, GPath destPath) throws IOException {
    if (!exists(destPath.getParent())) {
      if (!mkdirs(destPath.getParent())) {
        return false;
      }
    }
    return fs.rename(new Path(targetPath.toString()), new Path(destPath
        .toString()));
  }

  public boolean isDirectory(GPath path) throws IOException {
    return fs.getFileStatus(new Path(path.toString())).isDir();
  }

  public long getLength(GPath path) throws IOException {
    return fs.getFileStatus(new Path(path.toString())).getLen();
  }

  public DataInputStream openDataInputStream(GPath path) throws IOException {
    return fs.open(new Path(path.toString()));
  }

  public DataInputStream openDataInputStream(GPath path, int bufferSize)
      throws IOException {
    return fs.open(new Path(path.toString()), bufferSize);
  }

  public boolean renameTo(GPath targetPath, GPath destPath) throws IOException {
    return fs.rename(new Path(targetPath.toString()), new Path(destPath
        .toString()));
  }

  public void close() throws IOException {

  }

  public void setPermission(GPath path, short mode) throws IOException {
    fs.setPermission(new Path(path.toString()), new FsPermission(mode));
  }

  public boolean delete(GPath path, boolean recursive) throws IOException {
    Path hadoopPath = new Path(path.toString());
    if (!fs.exists(hadoopPath)) {
      return true;
    }
    FileStatus fileStatus = fs.getFileStatus(hadoopPath);
    if (fileStatus == null) {
      throw new IOException("No file:" + path);
    }

    return fs.delete(new Path(path.toString()), recursive);
  }
}
