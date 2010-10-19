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
public class LineKeyValueFileParser implements SystemMetricsParser {
  private static final String ALL = "*";

  private Set<String> fields = new HashSet<String>();

  private String fileName;
  
  public LineKeyValueFileParser() {
  }
  
  public void init(String fileName) throws IOException {
    this.fileName = fileName;
    String vmstatFields = CloudataMetricsFactory.getFactory().getAttribute("system." + fileName + ".field");

    if(vmstatFields != null) {
      if(ALL.equals(vmstatFields)) {
        fields.add("*");
      } else {
        String[] tokens = vmstatFields.split(",");
        
        for(String eachField: tokens) {
          fields.add(eachField);
        }
      }
    }
  }

  public Map<String, Object> getMetricsValues() {
    boolean all = false;
    
    if(fields.size() == 0 || fields.contains(ALL)) {
      all = true;
    }
    
    Map<String, Object> result = new HashMap<String, Object>();
    
    BufferedReader reader = null;
    
    try {
      reader = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));
      
      String line = null;
      
      while( (line = reader.readLine()) != null ) {
        if(line.trim().length() == 0) {
          continue;
        }
        int index = line.indexOf(":");
        if(index < 0) {
          index = line.indexOf(" ");
        } 
        if(index < 0) {
          CloudataMetricsFactory.LOG.error(fileName + " has't key field in:" + line);
          continue;
        }

        String fieldName = line.substring(0, index);
        
        if(all || fields.contains(fieldName)) {
          result.put(fieldName, line.substring(index + 1).trim());
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

}
