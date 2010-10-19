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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.cloudata.core.parallel.hadoop.DefaultTabletInputFormat;


/**
 * W = ( tf/(2.0 * ((1-0.75) + 0.75*(0.75*d-length/avg-d-length))) + tf) * log((N-n+0.5)/(n+0.5) )
 * 전체에서 미리 계산되어야 하는 값
 *  . sum of ducument length
 *  . number of documents
 * 하나의 Term에서 미리 계산되어야 하는 값
 * @author babokim
 *
 */
public class TermWeightJob {
  public static final Log LOG = LogFactory.getLog(TermWeightJob.class.getName()); 
  
  public void exec(String[] options) throws Exception {
    if(options.length < 1) {
      System.out.println("Usage: java TermWeightJob <num of repeats> termWeight <outputPath> [noGlobal]");
      System.exit(0);
    }
    if(options.length == 1 || !options[1].equals("noGlobal")) {
      TermGlobalJob termGlobalJob = new TermGlobalJob();
      termGlobalJob.exec();
    }
    Path outputPath = new Path(options[0]);
    
    JobConf jobConf = new JobConf(WebTableJob.class);
    jobConf.setJobName("TermWeightJob" + "(" + new Date() + ")");
    
    //<MAP>
    jobConf.setMapperClass(TermWeightMap.class);
    jobConf.setInputFormat(DefaultTabletInputFormat.class);
    jobConf.set(DefaultTabletInputFormat.INPUT_TABLE, WebTableJob.WEB_TABLE);
    jobConf.set(DefaultTabletInputFormat.INPUT_COLUMN_LIST, 
        WebTableJob.WEB_TABLE_COLUMNS[1] + "," + WebTableJob.WEB_TABLE_COLUMNS[2]);
    jobConf.setMapOutputKeyClass(Text.class);
    jobConf.setMapOutputValueClass(Text.class);    
//    jobConf.setMaxMapAttempts(0);
    //</MAP>

    //<REDUCE>
    jobConf.setReducerClass(TermWeightReduce.class);
    jobConf.setOutputKeyClass(Text.class);
    jobConf.setOutputValueClass(Text.class);    
    FileOutputFormat.setOutputPath(jobConf, outputPath);
    //jobConf.setMaxReduceAttempts(0);
    JobClient jobClinet = new JobClient(jobConf);
    int maxReduce = jobClinet.getClusterStatus().getMaxReduceTasks();
    
    jobConf.setNumReduceTasks(maxReduce);
    //</REDUCE>
    
    //Run Job
    JobClient.runJob(jobConf);
    
//    //delete temp output path
//    FileSystem fs = FileSystem.get(jobConf);
//    fs.delete(outputPath);
  }
}
