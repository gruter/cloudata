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
package org.cloudata.tools.cloudatafs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;

import org.cloudata.core.common.conf.CloudataConf;


/**
 * @author jindolk
 *
 */
public class CloudataFSShell {
  SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm");
  CloudataConf conf = new CloudataConf();
  CloudataFS fs;
  
  public static void main(String[] args) throws IOException {
    if(args.length < 1 || "-help".equals(args[0])) {
      printUsage();
      System.exit(0);
    }
    (new CloudataFSShell()).run(args);
  }
  
  public CloudataFSShell() throws IOException{
    fs = CloudataFS.get(conf);
  }
  
  private void run(String[] args) throws IOException {
    if("-ls".equals(args[0])) {
      ls(args);
    } else if("-lsr".equals(args[0])) {
      lsr(args);
    } else if("-get".equals(args[0])) {
        get(args);
    } else if("-put".equals(args[0])) {
      put(args);
    } else if("-rm".equals(args[0])) {
      rm(args);
    } else if("-rmr".equals(args[0])) {
      rmr(args);
    } else if("-mkdir".equals(args[0]) || "-mkdirs".equals(args[0])) {
      mkdirs(args);
    } else {
      printUsage();
    }    
  }

  private void rmr(String[] args) throws IOException {
    if(args.length < 2) {
      printUsage();
      return;
    }
    System.out.print("Warning!!!! rmr command will remove all datas! Continue rmr(Y|N): ");
    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    String answer = reader.readLine();

    if ("Y".equals(answer)) {
      delete(args[1], true);
    } else {
      System.out.println("rmr is cancelled");
    }
  }

  private void rm(String[] args) throws IOException {
    if(args.length < 2) {
      printUsage();
      return;
    }
    delete(args[1], false);
  }

  private void delete(String path, boolean recursive) throws IOException {
    if("/".equals(path)) {
      System.out.println("Cannot remove root");
      return;
    }
    
    if(!path.startsWith("/")) {
      path = "/" + path;
    }
    
    fs.delete(path, false);
  }
  
  private void mkdirs(String[] args) throws IOException {
    if(args.length < 2) {
      printUsage();
      return;
    }
    
    if("/".equals(args[1])) {
      System.out.println("Cannot make root");
      return;
    }
    
    String path = args[1];
    if(!path.startsWith("/")) {
      path = "/" + path;
    }
    fs.mkdirs(path);
  }
  
  private void put(String[] args) throws IOException {
    if(args.length < 3) {
      printUsage();
      return;
    }
    
    File file = new File(args[1]);
    if(!file.exists()) {
      System.out.println("Cannot access [" + args[1] + ": No such file or directory");
      return;
    }
    
    if(file.isDirectory()) {
      System.out.println("Cannot put [" + args[1] + ": can't put directory");
      return;
    } 
    
    String path = args[2];
    
    if(!fs.exists(CloudataFS.getParentPath(path))) {
      System.out.println("Cannot put [" + args[1] + ": parent directory [" + CloudataFS.getParentPath(path) + "] not exists");
      return;
    }
    
    FileNode fileNode = fs.getFileNode(path);
    if(fileNode != null) {
      System.out.println("Cannot access [" + path + ": Already exists");
      return;
    }
    
    copy(args[1], args[2], false);
    
  }
  
  private void get(String[] args) throws IOException {
    if(args.length < 3) {
      printUsage();
      return;
    }
    
    String path = args[1];
    FileNode fileNode = fs.getFileNode(path);
    if(fileNode == null) {
      System.out.println("Cannot access [" + path + ": No such file or directory");
      return;
    }
    
    if(fileNode.isDir()) {
      System.out.println("Cannot get [" + path + ": can't get directory");
      return;
    } 
    
    copy(args[1], args[2], true);
  }
  
  private void copy(String src, String dest, boolean fromLocal) throws IOException {
    BufferedOutputStream out = null;
    BufferedInputStream in = null;
    
    try {
      if(fromLocal) {
        in = new BufferedInputStream(fs.open(src));
        out = new BufferedOutputStream(new FileOutputStream(dest));
      } else {
        in = new BufferedInputStream(new FileInputStream(src));
        out = new BufferedOutputStream(fs.create(dest));
      }
      byte[] buf = new byte[64 * 1024];
      int readBytes = 0;
      while( (readBytes = in.read(buf)) > 0 ) {
        out.write(buf, 0, readBytes);
      }
    } finally {
      if(in != null) {
        try {
          in.close();
        } catch (IOException e) {
          throw e;
        }
      }
      
      if(out != null) {
        try { 
          out.close();
        } catch (IOException e) {
          throw e;
        }        
      }
    }
  }
  
  private void lsr(String[] args) throws IOException {
    String path;
    if(args.length > 1) {
      path = args[1];
    } else {
      path = "/";
    }

    int count = ls(path);    
    System.out.println("found " + count + " items.");
  }
  
  private int ls(String path) throws IOException {
    FileNode[] fileNodes = fs.listFileNode(path);
    if(fileNodes == null || fileNodes.length == 0) {
      return 0;
    }
    
    int result = 0;
    for(FileNode eachFileNode: fileNodes) {
      System.out.printf("%srw-rw-rw- %10s %s %s\n", 
          (eachFileNode.isDir() ? "d" : "-"),
          eachFileNode.getLength(),
          df.format(eachFileNode.getDate()),
          eachFileNode.getPath());
      if(eachFileNode.isDir()) {
        result += ls(eachFileNode.getPath());
      }
      result++;
    }
    
    return result;
  }
  
  private void ls(String[] args) throws IOException {
    String path;
    if(args.length > 1) {
      path = args[1];
    } else {
      path = "/";
    }
    CloudataConf conf = new CloudataConf();
    CloudataFS fs = CloudataFS.get(conf);
    FileNode[] fileNodes = fs.listFileNode(path);
    if(fileNodes == null || fileNodes.length == 0) {
      System.out.println("No such file or directory.");
      return;
    }
    
    System.out.println("found " + fileNodes.length + " items.");
    for(FileNode eachFileNode: fileNodes) {
      System.out.printf("%srw-rw-rw- %10s %s %s\n", 
          (eachFileNode.isDir() ? "d" : "-"),
          eachFileNode.getLength(),
          df.format(eachFileNode.getDate()),
          eachFileNode.getPath());
    }
  }
  
  private static void printUsage() {
    System.out.println("Usage: bin/cloudata fshell COMMAND");
    System.out.println("where COMMAND is one of:");
    System.out.println("  [-ls <path>]");
    System.out.println("  [-lsr <path>]");
    //System.out.println("  [-mv <src> <dst>]");
    //System.out.println("  [-cp <src> <dst>]");
    System.out.println("  [-rm <path>]");
    System.out.println("  [-rmr <path>]");
    System.out.println("  [-get <src> <localdst>]");
    System.out.println("  [-put <localsrc> ... <dst>]");
    //System.out.println("  [-cat <src>]");
    System.out.println("  [-mkdirs <path>]");
    System.out.println("  [-help [cmd]]");    
  }  
}
