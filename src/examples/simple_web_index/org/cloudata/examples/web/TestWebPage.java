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
package org.cloudata.examples.web;

public class TestWebPage {
  public static int MIN_TERM_LENGTH = 3;
  
  public static void main(String[] args) throws Exception {
    if(args.length < 2) {
      printUsgae();
    }
    
    int numOfRepeats = Integer.parseInt(args[0]);
    numOfRepeats = (numOfRepeats == 0 ? Integer.MAX_VALUE : numOfRepeats);
    
    String[] options = new String[args.length - 2];
    System.arraycopy(args, 2, options, 0, options.length);
    
    if(args[1].equals("webTable")) {
      for(int i = 0; i < numOfRepeats; i++) {
        WebTableJob job = new WebTableJob();
        job.exec(options);
      }
    } else if(args[1].equals("termWeight")) {
      TermWeightJob job = new TermWeightJob();
      job.exec(options);
    } else if(args[1].equals("termUpload")) {
      TermUploadJob job = new TermUploadJob();
      job.exec(options);
    } else if(args[1].equals("docFreq")) {
      DocFreqJob job = new DocFreqJob();
      job.exec(options);
    } else if(args[1].equals("termWeightOnline")) {
      TermWeightJobOnline job = new TermWeightJobOnline();
      job.exec(options);
    } else {
      printUsgae();
    }
  }
  
  private static void printUsgae() {
    System.out.println("Usage: java TestWebPage <num of repeats> <cmd> <options>");
    System.out.println("  <cmd>");
    System.out.println("    webTable: Upload Data file to T_WEB table(onloine)");
    System.out.println("    termWeight: store temp dfs term weigth(batch)");
    System.out.println("    docFreq: store doc freq to T_TERM(online)");
    System.out.println("    termWeightOnline: upload term weight to T_TERM(online)");
    System.out.println("    termUpload: batch upload T_TERM from termWeight result(batch)");
    System.out.println("  <job sequence>");
    System.out.println("    for batch : webTable -> termWeight -> termUpload");
    System.out.println("    for online: webTable -> docFreq -> termWeightOnline");
    
    System.exit(0);    
  }
}
