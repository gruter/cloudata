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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class ForkableProcess {
  List<String> command = new ArrayList<String>();
  ProcessBuilder builder;
  Process process;
  Thread processThread;
  Runnable stopMethod;
  int exitValue = -1;
  boolean verbose = true;

  public ForkableProcess(String classFullName, String... arguments) {
    this(new String[0], classFullName, arguments);
  }
  
  public ForkableProcess(String[] classpathList, String classFullName, String...arguments) {
    command.add(System.getProperty("java.home") + File.separator + "bin"
        + File.separator + "java");
    command.add("-cp");
    
    String classpath = "";
    classpath += System.getProperty("java.class.path");
    
    for(String path : classpathList) {
      classpath = classpath + File.pathSeparator + path;
    }
    
    //classpath += File.pathSeparator + System.getProperty("java.class.path");
    
    //printlnToStdout(classpath);
    
    command.add(classpath);
    
    command.add(classFullName);

    for (String arg : arguments) {
      command.add(arg);
    }

    builder = new ProcessBuilder(command);
    builder.redirectErrorStream(true);    
  }
  
  public void setVerbose(boolean flag) {
    this.verbose = flag;
  }

  public void setStopMethod(Runnable stopMethod) {
    this.stopMethod = stopMethod;
  }

  public void stop() {
    if (stopMethod != null) {
      stopMethod.run();
    } else {
      processThread.interrupt();
    }
  }

  public void start() {
    processThread = new Thread(new Runnable() {
      public void run() {
        try {
          process = builder.start();
          printlnToStdout("## " + process.toString() + " starts ##");
          BufferedReader br = new BufferedReader(new InputStreamReader(process
              .getInputStream()));
          String line;
          while ((line = br.readLine()) != null) {
            printlnToStdout("[" + process.toString() + "] " + line);
          }
          exitValue = process.waitFor();
          printlnToStdout("## " + process.toString() + " is finished ##");
        } catch (IOException e) {
          e.printStackTrace();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });

    processThread.start();
  }
  
  private void printlnToStdout(String msg) {
    if (verbose) {
      System.out.println(msg);
    }
  }
  
  public int getExitValue() {
    return exitValue;
  }

  public void waitForExit() throws InterruptedException {
    processThread.join();
  }
}
