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

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.scanner.ScannerFactory;
import org.cloudata.core.client.scanner.TableScanner;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.util.FileUtil;
import org.cloudata.core.common.util.FileUtil.InputParser;
import org.cloudata.core.fs.CloudataFileSystem;
import org.cloudata.core.fs.GPath;


/**
 * @author jindolk
 *
 */
public class TeraDataVerifier {
  public static void main(String[] args) throws IOException {
    if(args.length < 3) {
      System.out.println("Usage: java TeraDataVerifier <table name> <column name> <key file path>");
      System.exit(0);
    }
    
    CloudataConf conf = new CloudataConf();
    
    String mergeKeyFile = mergeKeyFile(conf, args[2]);
    String tableKeyFile = getKeyFromTable(conf, args[0], args[1]);
    
    BufferedReader keyReader = new BufferedReader(new InputStreamReader(new FileInputStream(mergeKeyFile)));
    BufferedReader tableKeyReader = new BufferedReader(new InputStreamReader(new FileInputStream(tableKeyFile)));
    try {
      int count = 1;
      boolean readLine1 = true;
      boolean readLine2 = true;
      
      String line1 = null;
      String line2 = null;
      while(true) {
        
        if(readLine1) {
          line1 = keyReader.readLine();
        }
        if(readLine2) {
          line2 = tableKeyReader.readLine();
        }
        if(line1 == null && line2 == null) {
          break;
        }
          
        if(line1 != null && line2 == null) {
          System.out.println("Unmatched_A:" + count + " [" + line1 + "][" + line2 + "]");
          break;
        }
        if(line1 == null && line2 != null) {
          System.out.println("Unmatched_B:" + count + " [" + line1 + "][" + line2 + "]");
          break;
        }
        
        if(!line1.equals(line2)) {
          System.out.println("Unmatched_C:" + count + " [" + line1 + "][" + line2 + "]");
          if(line1.compareTo(line2) > 0) {
            readLine1 = false;
            readLine2 = true;
          } else {
            readLine1 = true;
            readLine2 = false;
          }
        } else {
          readLine1 = true;
          readLine2 = true;
        }
        
        count++;
      }
      System.out.println( (count - 1) + " lines verified");
    } finally {
      keyReader.close();
      tableKeyReader.close();
    }
  }
  
  private static String getKeyFromTable(CloudataConf conf, String tableName, String columnName) throws IOException {
    String outputPath = "tableKeys.dat";
    File outputFile = new File(outputPath);
    if(outputFile.exists()) {
      System.out.println("use existed file:" + outputFile);
      return outputPath;
    }
    
    CTable ctable = CTable.openTable(conf, tableName);
    TableScanner scanner = ScannerFactory.openScanner(ctable, columnName);
    
    BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(outputPath));
    
    int count = 0;
    try {
      Row row = null;
      while( (row = scanner.nextRow()) != null ) {
        out.write((row.getKey() + "\n").getBytes());
        count++;
//        if(count % 100000 == 0) {
//          System.out.println(count + " rows scanned");
//        }
      }
      System.out.println("TableKey: " + count);
    } finally {
      scanner.close();
      out.close();
    }
    
    return outputPath;
  }
  
  private static String mergeKeyFile(CloudataConf conf, String keyPath) throws IOException {
    String outputPath = "mergedKeys.dat";
    
    File outputFile = new File(outputPath);
    
    if(outputFile.exists()) {
      System.out.println("use existed file:" + outputFile);
      return outputPath;
    }
    CloudataFileSystem fs = CloudataFileSystem.get(conf);
    
    if(!fs.exists(new GPath(keyPath)) || !fs.isDirectory(new GPath(keyPath))) {
      throw new IOException(keyPath + " not exist or not dir");
    }
    GPath[] paths = fs.list(new GPath(keyPath));
    
    List<String> pathList = new ArrayList<String>();
    for(GPath eachPath: paths) {
      if(eachPath.getName().indexOf("part-") < 0 || fs.isDirectory(eachPath)) {
        continue;
      }
      pathList.add(eachPath.toString());
    }
    
    InputParser parser = new InputParser() {
      @Override
      public String parse(String line) {
        String valueStr = line.trim();
        if(valueStr.indexOf("\t") > 0) {
          return valueStr.substring(0, valueStr.indexOf("\t"));
        } else {
          return valueStr;
        }
      }
    };
    System.out.println("OriginKey: " + FileUtil.mergeSort(fs, pathList.toArray(new String[]{}), outputPath, parser));
    return outputPath;
  }
}
