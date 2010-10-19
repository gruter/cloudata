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
package org.cloudata.core.tablet.backup;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.cloudata.core.client.CTable;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.fs.CloudataFileSystem;
import org.cloudata.core.fs.GPath;
import org.cloudata.core.parallel.hadoop.DefaultTabletInputFormat;


/**
 * cloudata에 저장된 데이터를 파일시스템으로 flat한 파일로 저장하는 MapReduce Job
 *
 */
public class BackupJob {
  /**
   * 백업 mapreduce job 수행
   * @param tableName
   * @param outputPath
   * @throws IOException
   */
  public void runBackUp(String tableName, String outputPath) throws IOException {
    CloudataConf nconf = new CloudataConf();
    CloudataFileSystem fs = CloudataFileSystem.get(nconf);
    if(fs.exists(new GPath(outputPath))) {
      throw new IOException("Output path already exists:" + outputPath);
    }
    
    if(!CTable.existsTable(nconf, tableName)) {
      throw new IOException("No Table:" + tableName);
    }
    
    CTable ctable = CTable.openTable(nconf, tableName);
    String columns = "";
    for(String eachColumn: ctable.getTableSchema().getColumnsArray()) {
      columns += eachColumn + ",";
    }
    columns = columns.substring(0, columns.length() - 1);
    
    String jobName = tableName + " backup";
    JobConf jobConf = new JobConf(BackupJob.class);
    jobConf.setJobName(jobName);
    
    jobConf.setMapperClass(BackupMap.class);
    jobConf.setInputFormat(BackupTabletInputFormat.class);
    jobConf.set(DefaultTabletInputFormat.INPUT_TABLE, tableName);
    jobConf.set(DefaultTabletInputFormat.INPUT_COLUMN_LIST, columns);
    FileOutputFormat.setOutputPath(jobConf, new Path(outputPath));
    jobConf.set("mapred.textoutputformat.separator", ",");
    jobConf.setOutputFormat(TextOutputFormat.class);
    
    //map only
    jobConf.setNumReduceTasks(0);
    
    JobClient.runJob(jobConf);
  }
  
  public static void main(String[] args) throws IOException {
    
  }
} 
