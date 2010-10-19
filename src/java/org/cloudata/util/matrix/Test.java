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
package org.cloudata.util.matrix;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.cloudata.core.client.Cell;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.io.CWritableUtils;
import org.cloudata.core.fs.CloudataFileSystem;
import org.cloudata.core.fs.GPath;


public class Test {
  public static void main(String[] args) throws IOException {
    Map<String, Integer> originValueMaps = new HashMap<String, Integer>();
    File file = new File("/home/cloudata/logdata");
    File[] files = file.listFiles();
    for(File eachFile: files) {
      if(!eachFile.getName().endsWith(".dat")) {
        continue;
      }
      BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(eachFile)));
      String line = null;
      while( (line = reader.readLine()) != null) {
        String[] tokens = line.split(" ");
        
        if(tokens.length == 4) {
          String rowKey = tokens[0];
          try {
            int value = Integer.parseInt(tokens[1]);
            
            int dup = Integer.parseInt(tokens[3]);
            if(dup != 0) {
              System.out.println("token is not 0:" + rowKey + "," + dup);
            }
            originValueMaps.put(rowKey, value);
          } catch (Exception e) {
            
          }
        }
      }
      reader.close();
    }
    
    file = new File("/home1/cloudata/cloudata");
    files = file.listFiles();
    
    Map<String, Integer> uploadedValueMaps = new HashMap<String, Integer>();
    for(File eachFile: files) {
      if(!eachFile.getName().startsWith("T_MUSIC_USER")) {
        continue;
      }
      BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(eachFile)));
      String line = null;
      
      while( (line = reader.readLine()) != null) {
        String[] tokens = line.split(",");
        if(tokens.length < 4) {
          continue;
        }
        String rowKey = tokens[2];
        String columnValue = tokens[3];
        String columnKey = columnValue.substring(0, columnValue.indexOf(":"));
        int index = columnValue.indexOf("(");
        int valueCount = 1;
        if(index >= 0) {
          String value = columnValue.substring(index + 1, columnValue.length() - 1);
          valueCount = Integer.parseInt(value);
        }
        if(uploadedValueMaps.containsKey(rowKey)) {
          uploadedValueMaps.put(rowKey, valueCount + uploadedValueMaps.get(rowKey));
        } else {
          uploadedValueMaps.put(rowKey, valueCount);
        }
      }
      reader.close();
    }
    
    int total = 0;
    for(Map.Entry<String, Integer> entry:originValueMaps.entrySet()) {
      String rowKey = entry.getKey();
      int value = entry.getValue();
      if(!uploadedValueMaps.containsKey(rowKey)) {
        System.out.println(">>>>>>No value:" + rowKey + "," + value);
      } else {
        if(value != uploadedValueMaps.get(rowKey)) {
          System.out.println(">>>>>>Not match:" + rowKey + "," + value + "," + uploadedValueMaps.get(rowKey));
        }
      }
      
      total +=  value;
    }
    
    int total2 = 0;
    for(Map.Entry<String, Integer> entry:uploadedValueMaps.entrySet()) {
      String rowKey = entry.getKey();
      int value = entry.getValue();
      
      total2 +=  value;
    }
    
    System.out.println("origin=" + total + ", uploaded=" + total2); 
  }
  
  public static void check() throws Exception {
    Map<String, Integer> valueMaps = new HashMap<String, Integer>();
    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("T_MUSIC_USER_2851184046447614.dat")));
    
    String line = null;
    
    while( (line = reader.readLine()) != null) {
      String[] tokens = line.split(",");
      if(tokens.length < 4) {
        continue;
      }
      String rowKey = tokens[2];
      String columnValue = tokens[3];
      String columnKey = columnValue.substring(0, columnValue.indexOf(":"));
      int index = columnValue.indexOf("(");
      int valueCount = 1;
      if(index >= 0) {
        String value = columnValue.substring(index + 1, columnValue.length() - 1);
        valueCount = Integer.parseInt(value);
      }
      
      valueMaps.put(rowKey + "," + columnKey, valueCount);
    }
    reader.close();
    
    CloudataFileSystem fs = CloudataFileSystem.get(new CloudataConf());
    GPath dataFilePath = new GPath("/user/cloudata/table/T_MUSIC_USER/T_MUSIC_USER_2851184046447614/item/2851185074900027/map.dat");
    DataInputStream din = fs.openDataInputStream(dataFilePath);
    
    Row.Key previousRowKey = null;
    Cell.Key previousCellKey = null;
    
    int valueCount = 0;
    
    Set<Long> timestamps = new HashSet<Long>();
    while(true) {
      try {
        //<Row.Key>
        int length = CWritableUtils.readVInt(din);
        if(Math.abs(length) > Constants.MAX_KEY_LENGTH) {
          System.out.println("Row.Key length too long[length=" + length);
          System.exit(0);
        }
        Row.Key rowKey = new Row.Key();
        byte[] rowKeyBytes = new byte[length];
        din.readFully(rowKeyBytes, 0, length);
        //</Row.Key>
        rowKey.set(rowKeyBytes);
        
        //<Cell.Key>
        length = CWritableUtils.readVInt(din);
        if(Math.abs(length) > Constants.MAX_KEY_LENGTH) {
          System.out.println("Cell.Key length too long[length=" + length);
          System.exit(0);
        }
        byte[] columnKeyBytes = new byte[length];
        din.readFully(columnKeyBytes, 0, length);
        Cell.Key columnKey = new Cell.Key(columnKeyBytes);
        //</Cell.Key>
        
        //<OpCode>
        int opCode = din.readInt();
        if(opCode != Constants.DELETEED && 
            opCode != Constants.INSERTED) {
          System.out.println("Wrong record operation code(DELETEED or INSERTED): code=" + opCode);
          System.exit(0);
        }
        //<//OpCode>
    
        //<Timestamp>
        long timestamp = din.readLong();
        new Date(timestamp);
        //</Timestamp>
    
        //<Value>
        int valueLength = din.readInt();
        if(Math.abs(valueLength) > 10 * 1024 * 1024) { //100MB
          System.out.println("Value length too long[length=" + length);
        }
        byte[] value = new byte[valueLength];
        if(valueLength > 0) {
          value = new byte[valueLength];
          din.readFully(value);
        } 
        
        if("worri3".equals(rowKey.toString()) && "1199869".equals(columnKey.toString())) {
          timestamps.add(timestamp);
          System.out.println(">>>" + opCode + ">" + timestamp + ">" + new String(value));
        }
        if(previousCellKey != null && !columnKey.equals(previousCellKey)) {
          String mapKey = previousRowKey.toString() + "," + previousCellKey.toString();
          if(!valueMaps.containsKey(mapKey)) {
            System.out.println(">>>>>>>>>>Not exists:" + mapKey);
          } else {
            int scanValueCount = valueMaps.get(mapKey);
            if(scanValueCount != valueCount) {
              System.out.println(">>>>>>>>>>Not matched:" + mapKey + "," + scanValueCount + "," + valueCount + "," + timestamps.size());
            }
          }
          valueCount = 0;
        }
        valueCount++;
        
        previousRowKey = rowKey;
        previousCellKey = columnKey;
        //</Value>
      } catch (EOFException eof) {
        break;
      }
    }
  }
}
