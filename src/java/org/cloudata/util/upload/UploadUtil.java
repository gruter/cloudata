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
package org.cloudata.util.upload;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.util.FileUtil;
import org.cloudata.core.parallel.hadoop.AbstractTabletInputFormat;
import org.cloudata.core.parallel.hadoop.CloudataMapReduceUtil;


/**
 * @author jindolk
 *
 */
public class UploadUtil {
  String inputPath;
  String tableName;
  String[] columns;
  int[] fieldNums;
  boolean keyValuePair;
  String delim;
  
  public static void main(String[] args) throws IOException {
    (new UploadUtil()).doUpload(new CloudataConf(), args);
  }
  
  private void printUsage() {
    System.out.println("Usage: UploadJob <options>");
    System.out.println(" -input=<inputPath>\tif input path starts with hdfs:// use MapReduce upload job");
    System.out.println(" -table=<tableName>\t target table name");
    System.out.println(" -column=<column,[column,...]>\tcomma seperator");
    System.out.println(" -field_num=<num1,[num2,...]>\tcomma seperator, field number in input file matched with column sequence, first num is rowkey field");
    System.out.println(" -key_value=Y|N\tinput record is key-value pair. if N, field stored in cell.key, default is Y");
    System.out.println(" -delim=<delim>\tfield delimiter");
  }
  
  public void doUpload(CloudataConf conf, String[] args) throws IOException {
    if(args.length < 1) {
      printUsage();
      return;
    }
    
    for(String eachArg: args) {
      String[] parsedArg = eachArg.split("=");
      if(parsedArg.length < 2) {
        continue;
      }
      
      if("-input".equals(parsedArg[0])) {
        inputPath = parsedArg[1];
      } else if("-table".equals(parsedArg[0])) {
        tableName = parsedArg[1];
      } else if("-column".equals(parsedArg[0])) {
        columns = parsedArg[1].split(",");
      } else if("-field_num".equals(parsedArg[0])) {
        String[] fieldNumStr = parsedArg[1].split(",");
        fieldNums = new int[fieldNumStr.length];
        for(int i = 0; i < fieldNumStr.length; i++) {
          fieldNums[i] = Integer.parseInt(fieldNumStr[i]);
        }
      } else if("-key_value".equals(parsedArg[0])) {
        keyValuePair = "Y".equals(parsedArg[1]);
      } else if("-delim".equals(parsedArg[0])) {
        delim = parsedArg[1];
      } 
    }
    
    if(inputPath == null){
      System.out.println("Error: check '-input' option");
      printUsage();
      return;
    }
    
    if(tableName == null){
      System.out.println("Error: check '-table' option");
      printUsage();
      return;
    }
    
    if(columns == null  || columns.length == 0){
      System.out.println("Error: check '-column' option");
      printUsage();
      return;
    }
    
    if(fieldNums == null  || fieldNums.length == 0){
      System.out.println("Error: check '-field_num' option");
      printUsage();
      return;
    }
    
    if(delim == null){
      System.out.println("Error: check '-delim' option");
      printUsage();
      return;
    }
    
    if(fieldNums.length != columns.length + 1) {
      System.out.println("Warn: not matched columns.length and field_num.length, field_num.length must matched columns.length + 1");
      return;
    }
    boolean hadoopJob = inputPath.startsWith("hdfs");
    
    if(hadoopJob) {
      doHadoopUpload(conf);
    } else {
      doLocalJob(conf);
    }
  }

  private void doLocalJob(CloudataConf conf) throws IOException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(inputPath)));
    
    try {
      String line;
      
      CTable ctable = CTable.openTable(conf, tableName);
      if(ctable == null) {
        throw new IOException("No table:" + tableName);
      }
      
      int count = 0;
      long timestamp = System.currentTimeMillis();
      while( (line = reader.readLine()) != null ) {

        putToTable(ctable, line, delim, columns, fieldNums, keyValuePair, timestamp);
        
        timestamp++;
        
        count++;
        if(count % 5000 == 0) {
          System.out.println(count + " rows inserted");
        }    
      }
      System.out.println("Total " + count + " rows inserted");
    } finally {
      if(reader != null) {
        reader.close();
      }
    }
  }

  protected static boolean putToTable(CTable ctable, String line, String delim, 
      String[] columns,
      int[] fieldNums, 
      boolean keyValuePair,
      long timestamp) throws IOException {
    String[] tokens = line.split(delim);
    
    Row row = new Row(new Row.Key(tokens[fieldNums[0]]));
    
    int tokenIndex = 1;
    for(int i = 0; i < columns.length; i++) {
      int index = fieldNums[tokenIndex++];
      String key = tokens[index];
      
      Cell.Key cellKey = Cell.Key.EMPTY_KEY;
      
      if(key.length() > 0) {
        cellKey = new Cell.Key(key);
      }
      
      byte[] valueByte = null;
      if(keyValuePair) {
        String value = tokens[index + 1];
        if(value.length() > 0) {
          valueByte = value.getBytes();
        }
      }
     
      row.addCell(columns[i], new Cell(cellKey, valueByte, timestamp));
    }
    ctable.put(row, false);
    
    return true;
  }
  
  private void doHadoopUpload(CloudataConf conf) throws IOException {
    if(!CTable.existsTable(conf, tableName)) {
      throw new IOException("No table:" + tableName);
    }
    
    JobConf jobConf = new JobConf(UploadUtil.class);
    String libDir = CloudataMapReduceUtil.initMapReduce(jobConf);
    
    jobConf.setJobName("UploadJob_" + tableName+ "(" + new Date() + ")");
    
    //KeyRangePartitioner를 사용하기 위해서는 반드시 
    //AbstractTabletInputFormat.OUTPUT_TABLE에 테이블명 지정
    jobConf.set(AbstractTabletInputFormat.OUTPUT_TABLE, tableName);
    
    //<Map>
    FileInputFormat.addInputPath(jobConf, new Path(inputPath));
    jobConf.setInputFormat(TextInputFormat.class);
    jobConf.set("uploadJob.delim", delim);
    String columnStr = "";
    for(String eachColumn: columns) {
      columnStr += eachColumn + ",";
    }
    jobConf.set("uploadJob.columns", columnStr);

    String fieldNumStr = "";
    for(int eachField: fieldNums) {
      fieldNumStr += eachField + ",";
    }
    jobConf.set("uploadJob.fieldNums", fieldNumStr);
    jobConf.setBoolean("uploadJob.keyValuePair", keyValuePair);
    jobConf.setMapperClass(UploadMap.class);
    jobConf.setMapOutputKeyClass(Text.class);
    jobConf.setMapOutputValueClass(Text.class);
    jobConf.setMapSpeculativeExecution(false);
    jobConf.setMaxMapAttempts(0);
    //</Map>
    
    //<Reduce>
    Path tempOutputPath = new Path("temp/uploadJob/" + tableName + "/reducer");
    FileOutputFormat.setOutputPath(jobConf, tempOutputPath);
    jobConf.setNumReduceTasks(0);
    //</Reduce>
    
    try {
      JobClient.runJob(jobConf);
    } finally {
      FileSystem fs = FileSystem.get(jobConf);
      FileUtil.delete(fs, tempOutputPath, true);
      CloudataMapReduceUtil.clearMapReduce(libDir);
    }
  }
}
