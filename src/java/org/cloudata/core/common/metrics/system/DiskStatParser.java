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
package org.cloudata.core.common.metrics.system;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.cloudata.core.common.metrics.CloudataMetricsFactory;


/**
 * @author jindolk
 *
 */
public class DiskStatParser implements SystemMetricsParser {
  /*
  Field  1 -- # of reads completed, This is the total number of reads completed successfully.
  Field  2 -- # of reads merged, 
  Field  3 -- # of sectors read
  Field  4 -- # of milliseconds spent reading
  Field  5 -- # of writes completed
  Field  6 -- # of writes merged
  Field  7 -- # of sectors written
  Field  8 -- # of milliseconds spent writing
  Field  9 -- # of I/Os currently in progress
  Field 10 -- # of milliseconds spent doing I/Os
  Field 11 -- weighted # of milliseconds spent doing I/Os
   */
  private static final String ALL = "*";
  private static int FILE_INDEX = 2;
  String[] filedNames = new String[]{
      "# of reads completed", 
      "# of reads merged",
      "# of sectors read",
      "# of milliseconds spent reading",
      "# of writes completed",
      "# of writes merged",
      "# of sectors written",
      "# of milliseconds spent writing",
      "# of I/Os currently in progress",
      "# of milliseconds spent doing I/Os",
      "weighted # of milliseconds spent doing I/Os"
      };

  String fileName;
  
  private Set<Integer> fields = new HashSet<Integer>();
  
  @Override
  public void init(String fileName) throws IOException {
    this.fileName = fileName;
    
    String vmstatFields = CloudataMetricsFactory.getFactory().getAttribute("system." + fileName + ".field");

    if(vmstatFields != null) {
      if(ALL.equals(vmstatFields)) {
        fields.add(-1);
      } else {
        String[] tokens = vmstatFields.split(",");
        
        try {
          for(String eachField: tokens) {
            fields.add(Integer.parseInt(eachField));
          }
        } catch (Exception e) {
          CloudataMetricsFactory.LOG.error("Wrong field property " + fileName + ": " + e.getMessage());
          return;
        }
      }
    }    
  }

  @Override
  public Map<String, Object> getMetricsValues() {
    boolean all = false;
    
    if(fields.size() == 0 && fields.contains(-1)) {
      all = true;
    }
    
    Map<String, Object> result = new HashMap<String, Object>();
    
    BufferedReader reader = null;
    
    try {
      reader = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));
      
      String line = null;
      
      while( (line = reader.readLine()) != null ) {
        String[] tokens = line.trim().split("[\\s]+");
        if("8".equals(tokens[0]) &&
            tokens[2].length() == 3) {
          if(tokens.length != 14) {
            continue;
          }

          for(int i = 1; i < 12; i++) {
            if(all || fields.contains(i)) {
              result.put(tokens[2] + "," + filedNames[i - 1], tokens[FILE_INDEX + i].trim());
            }
          }
        }
      }
    } catch (IOException e) {
      CloudataMetricsFactory.LOG.error("Can't read " + fileName + ": " + e.getMessage(), e);
    } finally {
      if(reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
        }
      }
    }
    
    return result;
  }
  
//  public static void main(String[] args) throws Exception {
//    String data = "   8    0 sda 1397861 366012 33637422 9661074 400267471 859889376 1492154766 2970600085 0 413447591 2987674754".trim();
//    
//    String[] tokens = data.split("[\\s]+");
//    System.out.println("Length: " + tokens.length);
//    
//    for(String eachToken: tokens) {
//      System.out.println(">>>" + eachToken);
//    }
//  }
}
