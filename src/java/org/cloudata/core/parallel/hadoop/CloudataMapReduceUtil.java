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
package org.cloudata.core.parallel.hadoop;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.util.FileUtil;
import org.cloudata.core.fs.CloudataFileSystem;
import org.cloudata.core.fs.GPath;


/**
 * Add cloudata's library to classpath for MapReduce job.
 */
public class CloudataMapReduceUtil {
  public static final Log LOG = LogFactory.getLog(CloudataMapReduceUtil.class.getName());
  static String CLOUDATA_JAR = "cloudata-[0-9]\\.[0-9](\\p{Alnum}|-|\\.)*\\.jar";
  static String ZOOKEEPER_JAR = "zookeeper-[0-9]\\.[0-9](\\p{Alnum}|-|\\.)*\\.jar";
  static String CLOUDATA_CONF = "cloudata-site.xml";
  
  static Configuration conf = new Configuration();
  
  /**
   * Configure classpath to run MapReduce job which uses Cloudata<BR>
   * 1. Uploads cloudata library to HDFS: cloudata-xxx-core.jar<BR>
   * 2. make cloudata-configure.jar includes cloudata-site.xml and uploads to HDFS<BR>
   * 3. run DistributedCache.addArchiveToClassPath() for each file<BR>
   * Must call clearMapReduce() after running job.
   * @param jobConf
   * @param cloudataHomeDir
   * @return temporary directory for linrary in HDFS. When calling clearMapReduce(), use this value.
   * @throws IOException
   */
  public static String initMapReduce(JobConf jobConf) throws IOException {
    
    String jarPath = "Cloudata_Lib_" + System.currentTimeMillis();
    
    FileSystem fs = FileSystem.get(conf);
    
    Path parentPath = fs.makeQualified(new Path(jarPath));
    //upload jar to dfs
    fs.mkdirs(parentPath);
    
    String[] classpaths = System.getProperty("java.class.path", "").split(File.pathSeparator);
    
    if(classpaths == null || classpaths.length == 0) {
      throw new IOException("No classpath");
    }
    
    List<String> uploadedFiles = new ArrayList<String>();
    for(String eachPath: classpaths) {
      if(eachPath.indexOf("cloudata") >= 0 || 
          eachPath.indexOf("lib") >= 0 ||
          eachPath.indexOf("conf") >= 0) {
        uploadFile(fs, parentPath, new File(eachPath), uploadedFiles);
      }
    }
    
    if(uploadedFiles.size() == 0) {
      throw new IOException("No lib files[cloudata-xxx-core.jar, zookeeper-xxx.jar] in classpath");
    }
    
    Path rootPath = new Path("/");
    String rootUri = fs.makeQualified(rootPath).toString();
    
    for(String eachPath: uploadedFiles) {
      Path path = fs.makeQualified(new Path(eachPath));
      String pathStr = path.toUri().toString();
      if(pathStr.indexOf(rootUri) >= 0) {
        pathStr = pathStr.substring(pathStr.indexOf(rootUri) + rootUri.length());
      }
      if(!pathStr.startsWith("/")) {
        pathStr = "/" + pathStr;
      }
      LOG.debug("DistributedCache.addArchiveToClassPath: " + pathStr);
      DistributedCache.addArchiveToClassPath(new Path(pathStr), jobConf);
    }
    return jarPath;
  }
  
  private static void uploadFile(FileSystem fs, Path parentPath, File file, List<String> uploadedFiles) throws IOException {
    if(file.isDirectory()) {
      File[] files = file.listFiles();
      if(files == null || files.length == 0) {
        return;
      }    
      for(File eachFile: files) {
        uploadFile(fs, parentPath, eachFile, uploadedFiles);
      }
    } else {
      String fileName = file.getName();
      String fullPath = file.getPath();
      boolean matched = false;
      
      if(fileName.matches(CLOUDATA_JAR)) {
        matched = true;
      } else if(fileName.matches(ZOOKEEPER_JAR)) {
        matched = true;
      } else if(fileName.matches(CLOUDATA_CONF)) {
        //jar 파일을 만들면서 upload
        Path uploadPath = makeJarToHDFS(fs, parentPath, file);
        uploadedFiles.add(uploadPath.toUri().toString());
      }
      
      if(matched) {
        Path uploadPath = new Path(parentPath, fileName);
        OutputStream out = fs.create(uploadPath);
        FileUtil.dumpFile(fullPath, out);
        out.close();
        uploadedFiles.add(uploadPath.toUri().toString());
      }
    }    
  }

  private static Path makeJarToHDFS(FileSystem fs, Path parentPath, File file) throws IOException {
    Path path = new Path(parentPath, file.getName() + ".jar");
    
    JarOutputStream out = new JarOutputStream(fs.create(path));
    out.putNextEntry(new JarEntry(file.getName()));
    
    BufferedInputStream in = new BufferedInputStream(new FileInputStream(file));
    
    byte[] buf = new byte[1024];
    
    try {
      int readBytes = 0;
      while( (readBytes = in.read(buf)) > 0 ) {
        out.write(buf, 0, readBytes);
      }
    } finally {
      if(out != null) {
        out.close();
      }
      
      if(in != null) {
        in.close();
      }
    }
    
    return path;
  }

  /**
   * Clear temporary library directory.
   * @param jarPath
   * @throws IOException
   */
  public static void clearMapReduce(String jarPath) throws IOException {
    CloudataFileSystem fs = CloudataFileSystem.get(new CloudataConf());
    GPath parentPath = new GPath(jarPath);
    fs.delete(parentPath, true);
  }
  
  
  public static int getMapNum(JobConf jobConf) {
    String taskId = jobConf.get("mapred.task.id");
    String taskId2 = taskId.substring(taskId.length() - 8);
    return Integer.parseInt(taskId2.substring(0,6));
  }
  
  public static void main(String[] args) throws Exception {

  }
}
