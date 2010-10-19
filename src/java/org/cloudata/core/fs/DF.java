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
import java.util.StringTokenizer;

import org.cloudata.core.common.ShellCommand;
import org.cloudata.core.common.conf.CloudataConf;


/** Filesystem disk space usage statistics.  Uses the unix 'df' program.
 * Tested on Linux, FreeBSD, Cygwin. */
public class DF extends ShellCommand {
  public static final long DF_INTERVAL_DEFAULT = 10 * 1000; // default DF refresh interval 
  
  private String  dirPath;
  private DiskInfo diskInfo = new DiskInfo();
  private Thread refreshThread;
  private long refreshInterval;
  private boolean shouldRun = true;
  
  public DF(File path, CloudataConf conf) throws IOException {
    this(path, conf.getLong("dfs.df.interval", DF.DF_INTERVAL_DEFAULT));
  }

  public DF(File path, long dfInterval) throws IOException {
    this.dirPath = path.getCanonicalPath();
    this.refreshInterval = dfInterval;
    this.run();
  }

  class DFRefreshThread implements Runnable {
    
    public void run() {
      
      if(onlyUnix() && WINDOWS) {
        LOG.warn("Can't run df on WINDOWs");
        return;
      }
      while(shouldRun) {
        try {
          Thread.sleep(refreshInterval);
          
          try {
            DF.this.run();
          } catch (IOException e) {
            LOG.warn("Could not get disk usage information", e);
          }
          //System.out.println(DF.this.toString());
        } catch (InterruptedException e) {
        }
      }
    }
  }
  
  /**
   * Start the disk usage checking thread.
   */
  public void start() {
    //only start the thread if the interval is sane
    if(refreshInterval > 0) {
      refreshThread = new Thread(new DFRefreshThread(), 
          "refreshUsed-"+dirPath);
      refreshThread.setDaemon(true);
      refreshThread.start();
    }
  }
  
  /**
   * Shut down the refreshing thread.
   */
  public void shutdown() {
    this.shouldRun = false;
    
    if(this.refreshThread != null) {
      this.refreshThread.interrupt();
    }
  }
  
  /// ACCESSORS
  public String getDirPath() {
    return dirPath;
  }
  
  public String getFilesystem() throws IOException { 
    return diskInfo.filesystem; 
  }
  
  public long getCapacity() throws IOException { 
    return diskInfo.capacity; 
  }
  
  public long getUsed() throws IOException { 
    return diskInfo.used;
  }
  
  public long getAvailable() throws IOException { 
    return diskInfo.available;
  }
  
  public int getPercentUsed() throws IOException {
    return diskInfo.percentUsed;
  }

  public String getMount() throws IOException {
    return diskInfo.mount;
  }
  
  public String toString() {
    return
      "df -k " + diskInfo.mount +"\n" +
      diskInfo.filesystem + "\t" +
      diskInfo.capacity / 1024 + "\t" +
      diskInfo.used / 1024 + "\t" +
      diskInfo.available / 1024 + "\t" +
      diskInfo.percentUsed + "%\t" +
      diskInfo.mount;
  }

  public DiskInfo getDiskInfo() {
    return diskInfo;
  }
  
  protected String[] getExecString() {
    // ignoring the error since the exit code it enough
    return new String[] {"bash","-c","exec 'df' '-k' '" + dirPath 
                         + "' 2>/dev/null"};
  }
  
  protected void parseExecResult(BufferedReader lines) throws IOException {
    lines.readLine();                         // skip headings
  
    String line = lines.readLine();
    if (line == null) {
      throw new IOException( "Expecting a line not the end of stream" );
    }
    StringTokenizer tokens =
      new StringTokenizer(line, " \t\n\r\f%");
    
    this.diskInfo.filesystem = tokens.nextToken();
    if (!tokens.hasMoreTokens()) {            // for long filesystem name
      line = lines.readLine();
      if (line == null) {
        throw new IOException( "Expecting a line not the end of stream" );
      }
      tokens = new StringTokenizer(line, " \t\n\r\f%");
    }
    this.diskInfo.capacity = Long.parseLong(tokens.nextToken()) * 1024;
    this.diskInfo.used = Long.parseLong(tokens.nextToken()) * 1024;
    this.diskInfo.available = Long.parseLong(tokens.nextToken()) * 1024;
    this.diskInfo.percentUsed = Integer.parseInt(tokens.nextToken());
    this.diskInfo.mount = tokens.nextToken();
  }

  public boolean onlyUnix() {
    return true;
  }
  
  public static void main(String[] args) throws Exception {
    String path = ".";
    if (args.length > 0)
      path = args[0];

    DF df = new DF(new File(path), DF_INTERVAL_DEFAULT);
    df.start();
    //System.out.println(df.toString());
  }
}
