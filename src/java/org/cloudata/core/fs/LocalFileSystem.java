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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.util.FileUtil;


/**
 * 로컬 파일시스템 구성을 지원하는 파일시스템 주로 테스트 용도로 활용
 * 
 * @author babokim
 * 
 */
public class LocalFileSystem extends CloudataFileSystem {

  protected LocalFileSystem(CloudataConf nconf) throws IOException {
    super(nconf);
  }

  public boolean exists(GPath path) {
    File file = new File(path.toString());
    return file.exists();
  }

  public boolean mkdirs(GPath path) {
    File file = new File(path.toString());
    // System.out.println("Local Filesystem mkdirs:" + path);
    return file.mkdirs();
  }

  public GPath[] list(GPath path) throws IOException {
    File file = new File(path.toString());
    if (!file.isDirectory())
      return null;

    File[] files = file.listFiles();

    if (files == null) {
      return null;
    }

    GPath[] paths = new GPath[files.length];
    for (int i = 0; i < files.length; i++) {
      paths[i] = new GPath(files[i].getAbsolutePath());
    }

    return paths;
  }

  public OutputStream create(GPath path) throws IOException {
    File file = new File(path.toString());
    if (file.getParentFile() != null && !file.getParentFile().exists()) {
      if (!file.getParentFile().mkdirs()) {
        if (!file.getParentFile().exists()) {
          throw new IOException("Can't make directory:" + file.getParentFile());
        } else {
          if (!file.getParentFile().mkdirs()) {
            throw new IOException("Can't make directory:"
                + file.getParentFile());
          }
        }
      }
    }
    return new FileOutputStream(file);
  }

  public OutputStream create(GPath path, short replication) throws IOException {
    return create(path); 
  }
  
  public OutputStream create(GPath path, int bufferSize) throws IOException {
    return new BufferedOutputStream(create(path), bufferSize);
  }

  public OutputStream create(GPath path, int bufferSize, short replication) throws IOException {
    return create(path, bufferSize);
  }
  
  public InputStream open(GPath path) throws IOException {
    File file = new File(path.toString());
    return new FileInputStream(file);
  }

  public InputStream open(GPath path, int bufferSize) throws IOException {
    return new BufferedInputStream(open(path), bufferSize);
  }

  public void seek(InputStream in, long offset) throws IOException {
    in.skip(offset);
  }

  public boolean delete(GPath path) throws IOException {
    File file = new File(path.toString());

    boolean result = file.delete();
    return result;
  }

  public boolean move(GPath targetPath, GPath destPath) throws IOException {
    if (!exists(destPath.getParent())) {
      if (!mkdirs(destPath.getParent())) {
        return false;
      }
    }
    return FileUtil.moveFile(targetPath.toString(), destPath.toString());
  }

  public boolean isDirectory(GPath path) throws IOException {
    File file = new File(path.toString());
    return file.isDirectory();
  }

  public long getLength(GPath path) throws IOException {
    File file = new File(path.toString());
    return file.length();
  }

  public DataInputStream openDataInputStream(GPath path) throws IOException {
    return new DataInputStream(open(path));
  }

  public DataInputStream openDataInputStream(GPath path, int bufferSize)
      throws IOException {
    return new DataInputStream(new BufferedInputStream(open(path), bufferSize));
  }

  public boolean renameTo(GPath targetPath, GPath destPath) throws IOException {
    return rename(new File(targetPath.toString()),
        new File(destPath.toString()));
  }

  private boolean rename(File fromFile, File toFile) throws IOException {
    if (fromFile.isDirectory()) {
      File[] files = fromFile.listFiles();
      if (files == null) {
        // 디렉토리 내 아무것도 없는 경우
        return fromFile.renameTo(toFile);
      } else {
        // 디렉토리내 파일 또는 디렉토리가 존재하는 경우
        if (!toFile.mkdirs()) {
          return false;
        }
        for (File eachFile : files) {
          File toFileChild = new File(toFile, eachFile.getName());
          if (eachFile.isDirectory()) {
            if (!rename(eachFile, toFileChild)) {
              return false;
            }
          } else {
            if (!eachFile.renameTo(toFileChild)) {
              return false;
            }
          }
        }
        return fromFile.delete();
      }
    } else {
      // 파일인 경우
      if (fromFile.getParent() != null) {
        if (!toFile.mkdirs()) {
          return false;
        }
      }
      return fromFile.renameTo(toFile);
    }
  }

  // @Override
  // public LocatedBlocks getBlockInfos(GPath path) throws IOException {
  // return null;
  // }
  //
  // @Override
  // public DataInputStream openDataInputStream(GPath path, LocatedBlocks
  // dfsBlockInfos) throws IOException {
  // return openDataInputStream(path);
  // }

  public void close() throws IOException {

  }

  public String[][] getFileCacheHints(GPath path) throws IOException {
    return new String[][] { new String[] { "127.0.0.1:50060" } };
  }

  public void setPermission(GPath path, short mode) throws IOException {

  }

  public boolean delete(GPath path, boolean recursive) throws IOException {
    if (!exists(path)) {
      return true;
    }
    if (isDirectory(path)) {
      GPath[] srcs = list(path);
      int length = (srcs != null ? srcs.length : 0);
      for (int i = 0; i < length; i++) {
        if (recursive) {
          if (isDirectory(srcs[i])) {
            if (!delete(srcs[i], recursive)) {
              return false;
            }
          } else {
            if (!delete(srcs[i])) {
              return false;
            }
          }
          // deleteAll(srcs[i], recursive);
        } else {
          if (!delete(srcs[i])) {
            return false;
          }
        }
      }
    }
    if (!delete(path)) {
      return false;
    }

    return true;
  }
  
  public void setReplication(GPath path, short replication) throws IOException {
  }
}
