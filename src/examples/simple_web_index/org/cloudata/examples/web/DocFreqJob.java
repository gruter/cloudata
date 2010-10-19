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

import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.cloudata.core.client.CTable;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.parallel.hadoop.AbstractTabletInputFormat;
import org.cloudata.core.tablet.TableSchema;


public class DocFreqJob {
  public static final Log LOG = LogFactory.getLog(TermWeightJob.class.getName()); 
  
  public void exec(String[] options) throws Exception {
    if(options.length < 1) {
      System.out.println("Usage: java DocFreqJob <num of repeats> docFreq [#reduce]");
      System.exit(0);
    }

    JobConf jobConf = new JobConf(DocFreqJob.class);
    
    JobClient jobClinet = new JobClient(jobConf);
    int maxReduce = jobClinet.getClusterStatus().getMaxReduceTasks() * 2;
    if(options.length > 0) {
      maxReduce = Integer.parseInt(options[0]);
    }

    CloudataConf nconf = new CloudataConf();
    if(!CTable.existsTable(nconf, TermUploadJob.TERM_TABLE)) {
      TableSchema temrTableInfo = new TableSchema(TermUploadJob.TERM_TABLE, "Test", TermUploadJob.TERM_TABLE_COLUMNS);
      CTable.createTable(nconf, temrTableInfo);
    }
    Path tempOutputPath = new Path("DocFreqJob_" + System.currentTimeMillis());
    
    jobConf.setJobName("DocFreqJob" + "(" + new Date() + ")");
    
    //<MAP>
    jobConf.setMapperClass(DocFreqMap.class);
    jobConf.setInputFormat(AbstractTabletInputFormat.class);
    jobConf.set(AbstractTabletInputFormat.INPUT_TABLE, WebTableJob.WEB_TABLE);
    jobConf.set(AbstractTabletInputFormat.INPUT_COLUMN_LIST, 
        WebTableJob.WEB_TABLE_COLUMNS[1] + "," + WebTableJob.WEB_TABLE_COLUMNS[2]);
    jobConf.setMapOutputKeyClass(Text.class);
    jobConf.setMapOutputValueClass(Text.class);    
//    jobConf.setMaxMapAttempts(0);
    //</MAP>

    //<REDUCE>
    jobConf.setReducerClass(DocFreqReduce.class);
    jobConf.setOutputKeyClass(Text.class);
    jobConf.setOutputValueClass(Text.class);    
    FileOutputFormat.setOutputPath(jobConf, tempOutputPath);
    jobConf.setNumReduceTasks(maxReduce);
    //</REDUCE>
    
    //Run Job
    JobClient.runJob(jobConf);
    
//    //delete temp output path
    FileSystem fs = FileSystem.get(jobConf);
    fs.delete(tempOutputPath, true);
  }
}
