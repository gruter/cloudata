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
package org.cloudata.core.testjob.tera;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CellFilter;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.RowFilter;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.fs.CloudataFileSystem;
import org.cloudata.core.fs.GPath;


/**
 * @author jindolk
 *
 */
public class TeraReadLocalJob {
  public static final Log LOG = LogFactory.getLog(TeraReadLocalJob.class.getName()); 
  public static void main(String[] args) throws IOException {
    CloudataConf conf = new CloudataConf();
    conf.set("cloudata.filesystem", "local");
    if(args.length < 3) {
      System.out.println("Usage: java TeraReadLocalJob <table name> <keyOutputPath> <thread num>");
      System.exit(0);
    }
    
    String tableName = args[0];
    String keyOutputPath = args[1];
    
    CloudataFileSystem fs = CloudataFileSystem.get(conf);
    
    CTable ctable = CTable.openTable(conf, tableName);
    if(ctable == null) {
      System.out.println("No Table " + tableName);
      System.exit(0);
    }

    int numThread = Integer.parseInt(args[2]);
    
    for(int i = 0; i < numThread; i++) {
      (new ReadThread(i, fs, ctable, keyOutputPath)).start();
    }
  }
  
  static class ReadThread extends Thread {
    int id;
    CloudataFileSystem fs;
    CTable ctable;
    String keyOutputPath;
    public ReadThread(int id, CloudataFileSystem fs, CTable ctable, String keyOutputPath) {
      this.id = id;
      this.fs = fs;
      this.ctable = ctable;
      this.keyOutputPath = keyOutputPath;
    }
    
    public void run() {
      try {
        GPath[] paths = fs.list(new GPath(keyOutputPath));
        
        if(paths == null || paths.length == 0) {
          System.out.println("No files in " + keyOutputPath);
          System.exit(0);
        }

        List<GPath> targetPaths = new ArrayList<GPath>();
        for(GPath eachPath: paths) {
          if(eachPath.toString().indexOf("part-") >= 0) {
            targetPaths.add(eachPath);
          }
        }
        
        int size = targetPaths.size();
        Random rand = new Random(System.currentTimeMillis());
        Set<Integer> finishedPaths = new HashSet<Integer>();
        while(true) {
          GPath targetPath = null;
          while(true) {
            int nextIndex = rand.nextInt(size);
            if(!finishedPaths.contains(nextIndex)) {
              finishedPaths.add(nextIndex);
              targetPath = targetPaths.get(nextIndex);
              break;
            }
          }
          read(fs, ctable, targetPath);
          if(finishedPaths.size() == targetPaths.size()) {
            break;
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    } 

    private void read(CloudataFileSystem fs, CTable ctable, GPath path) throws IOException {
      LOG.info("[" + id + "] starts " + path);
      BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
      
      try {
        String line = null;
        int count = 0;
        long timeSum = 0;
        long totalTimeSum = 0;
        while( (line = reader.readLine()) != null ) {
          String valueStr = line.toString().trim();
          if(valueStr.indexOf("\t") > 0) {
            valueStr = valueStr.substring(0, valueStr.indexOf("\t"));
          }
          Row.Key rowKey = new Row.Key(valueStr);
          RowFilter rowFilter = new RowFilter(rowKey);
          rowFilter.addCellFilter(new CellFilter("Col1"));
          long startTime = System.currentTimeMillis();
          Row row = ctable.get(rowFilter);
          long gap = (System.currentTimeMillis() - startTime);
          timeSum += gap;
          totalTimeSum += (System.currentTimeMillis() - startTime);
          if(row == null) {
            throw new IOException("No Data:[" + rowKey + "]");
          }
          if(!row.getKey().equals(rowKey)) {
            throw new IOException("Not equals Data:[" + row.getKey() + "," + rowKey + "]");
          }
          Cell cell = row.getFirst("Col1");
          if(cell == null) {
            throw new IOException("No Cell Data:[" + rowKey + "]");
          } 
          count++;
          if(count % 1000 == 0) {
            LOG.info("[" + id + "] read: " + count + ", last row key[" + rowKey + "], " +
            		"1000s avg. time[" + (timeSum/1000) + "], " +
            				"total avg. time[" + (totalTimeSum/count) + "]");
            timeSum = 0;
          }
        }
        LOG.info("[" + id + "] " + count + " row read from " + path);
      } finally {
        reader.close();
      }
    }
  }
}
