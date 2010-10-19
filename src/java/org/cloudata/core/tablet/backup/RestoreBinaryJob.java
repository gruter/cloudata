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
import java.util.ArrayList;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileAsBinaryInputFormat;
import org.cloudata.core.client.CTable;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.fs.GPath;
import org.cloudata.core.parallel.hadoop.DefaultTabletInputFormat;
import org.cloudata.core.tablet.TableSchema;


/**
 * @author jindolk
 *
 */
public class RestoreBinaryJob {

  /**
   * @param string
   * @param string2
   * @param binary
   */
  public void runRestore(String tableName, String[] columnNames, 
      int numOfVersion, String inputPath) throws IOException {
    CloudataConf nconf = new CloudataConf();

    JobConf partitionJob = new JobConf(BackupJob.class);
    
    FileSystem fs = FileSystem.get(partitionJob);
    
    if(!fs.exists(new Path(inputPath))) {
      throw new IOException("input path not exists:" + inputPath);
    }
    
    if(CTable.existsTable(nconf, tableName)) {
      throw new IOException("table already exists" + tableName);
    }
    
    TableSchema tableSchema = new TableSchema(tableName, "", columnNames);
    tableSchema.setNumOfVersion(numOfVersion);
    CTable.createTable(nconf, tableSchema);
    
    String columns = "";
    for(String eachColumn: columnNames) {
      columns += eachColumn.trim() + ",";
    }
    columns = columns.substring(0, columns.length() - 1);
    
    String jobName = tableName + " restore";
    String tempDir = jobName + "_" + System.currentTimeMillis();

    partitionJob.setJobName(tableName + " restore");
    
    partitionJob.setMapperClass(RestoreBinaryPartitionMap.class);
    FileInputFormat.addInputPath(partitionJob, new Path(inputPath));
    partitionJob.setInputFormat(RestoreSequenceFileAsBinaryInputFormat.class);
    partitionJob.set(DefaultTabletInputFormat.OUTPUT_TABLE, tableName);
    FileOutputFormat.setOutputPath(partitionJob, new Path(tempDir));
    
    //map only
    partitionJob.setNumReduceTasks(0);
    
    JobClient.runJob(partitionJob);
    
    //delete temp output dir
    fs.delete(new Path(tempDir), true);

    ////////////////////////////////////////////////////////////////
    JobConf jobConf = new JobConf(BackupJob.class);
    jobConf.setJobName(tableName + " restore");
    
    jobConf.setMapperClass(RestoreBinaryMap.class);
    FileInputFormat.addInputPath(jobConf, new Path(inputPath));
    jobConf.setInputFormat(RestoreSequenceFileAsBinaryInputFormat.class);
    jobConf.set(DefaultTabletInputFormat.OUTPUT_TABLE, tableName);
    jobConf.set(DefaultTabletInputFormat.INPUT_COLUMN_LIST, columns);
    FileOutputFormat.setOutputPath(jobConf, new Path(tempDir));
    jobConf.setMaxMapAttempts(0);
    //map only
    jobConf.setNumReduceTasks(0);
    
    JobClient.runJob(jobConf);
    
    //delete temp output dir
    fs.delete(new Path(tempDir), true);
  }

  /**
   * File 1개에 Map하나가 할당되도록
   * @author jindolk
   *
   */
  static class RestoreSequenceFileAsBinaryInputFormat extends SequenceFileAsBinaryInputFormat {
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
      FileStatus[] files = listStatus(job);
      // generate splits
      ArrayList<FileSplit> splits = new ArrayList<FileSplit>(numSplits);
      for (int i = 0; i < files.length; i++) {
        FileStatus fileStatus = files[i];
        Path file = fileStatus.getPath();
        long length = fileStatus.getLen();
        if (length != 0) {
          splits.add(new FileSplit(file, 0, length, (String[])null));
        }
      }
      return splits.toArray(new FileSplit[splits.size()]);
    }
  }
}
