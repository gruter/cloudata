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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.common.ShellCommand;
import org.cloudata.core.common.conf.CloudataConf;


/** Filesystem disk space usage statistics.  Uses the unix 'du' program*/
public class DiskUsage extends ShellCommand {
  public static final Log LOG = LogFactory.getLog(DiskUsage.class.getName());
  
  private String  dirPath;

  private AtomicLong used = new AtomicLong();
  
  /**
   * Keeps track of disk usage.
   * @param path the path to check disk usage in
   * @param interval refresh the disk usage at this interval
   * @throws IOException if we fail to refresh the disk usage
   */
  public DiskUsage(File path, long interval) throws IOException {
    //we set the Shell interval to 0 so it will always run our command
    //and use this one to set the thread sleep interval
    this.dirPath = path.getCanonicalPath();
    
    //populate the used variable
    run();
  }
  
  /**
   * Keeps track of disk usage.
   * @param path the path to check disk usage in
   * @param conf configuration object
   * @throws IOException if we fail to refresh the disk usage
   */
  public DiskUsage(File path, CloudataConf conf) throws IOException {
    this(path, 5000L);
  }

  /**
   * @return disk space used 
   * @throws IOException if the shell command fails
   */
  public long getUsed() throws IOException {
    run();
    return used.longValue();
  }

  /**
   * @return the path of which we're keeping track of disk usage
   */
  public String getDirPath() {
    return dirPath;
  }
  
  public String toString() {
    return
      "du -sk " + dirPath +"\n" +
      used + "\t" + dirPath;
  }

  protected String[] getExecString() {
    return new String[] {"du", "-sk", dirPath};
  }
  
  public boolean onlyUnix() {
    return true;
  }
  
  protected void parseExecResult(BufferedReader lines) throws IOException {
    String line = lines.readLine();
    if (line == null) {
      throw new IOException("Expecting a line not the end of stream");
    }
    String[] tokens = line.split("\t");
    if(tokens.length == 0) {
      throw new IOException("Illegal du output");
    }
    this.used.set(Long.parseLong(tokens[0])*1024);
  }

  public static void main(String[] args) throws Exception {
    String path = ".";
    if (args.length > 0) {
      path = args[0];
    }
    System.out.println(new DiskUsage(new File(path), new CloudataConf()).toString());
  }
}
