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

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.cloudata.core.client.CellFilter;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.RowFilter;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.util.FileUtil;
import org.cloudata.core.parallel.hadoop.AbstractTabletInputFormat;
import org.cloudata.core.parallel.hadoop.DefaultTabletInputFormat;
import org.cloudata.core.parallel.hadoop.InputTableInfo;


public class TeraScanJob {
  public static final Log LOG = LogFactory.getLog(TeraScanJob.class.getName()); 
  
  public void runJob(String tableName) throws IOException {
    JobConf jobConf = new JobConf(TeraScanJob.class);
    
    CloudataConf nconf = new CloudataConf();
    
    if(!CTable.existsTable(nconf, tableName)) {
      System.out.println("No table:" + tableName);
      System.exit(0);
    }
    Path tempOutputPath = new Path("TeraScanJob" + System.currentTimeMillis());
    
    jobConf.setJobName("TeraScanJob" + "(" + new Date() + ")");
    
    //<MAP>
    jobConf.setMapperClass(TeraScanMap.class);
    jobConf.setInputFormat(TeraScanJobTabletInputFormat.class);
    jobConf.set(AbstractTabletInputFormat.INPUT_TABLE, tableName);
    jobConf.setMapSpeculativeExecution(false);
    jobConf.setMaxMapAttempts(0);
    //</MAP>

    //<REDUCE>
//    jobConf.setReducerClass(DocFreqReduce.class);
//    jobConf.setOutputKeyClass(Text.class);
//    jobConf.setOutputValueClass(Text.class);    
    FileOutputFormat.setOutputPath(jobConf, tempOutputPath);
    jobConf.setNumReduceTasks(0);
    //</REDUCE>
    
    //Run Job
    JobClient.runJob(jobConf);
    
//    //delete temp output path
    FileSystem fs = FileSystem.get(jobConf);
    FileUtil.delete(fs, tempOutputPath, true);
  }
  
  static class TeraScanJobTabletInputFormat extends AbstractTabletInputFormat {
    public TeraScanJobTabletInputFormat() throws IOException {
      super();
    }

    @Override
    public InputTableInfo[] getInputTableInfos(JobConf jobConf) {
      String tableName = jobConf.get(DefaultTabletInputFormat.INPUT_TABLE);
      RowFilter rowFilter = new RowFilter();
      rowFilter.addCellFilter(new CellFilter("Col1"));
      
      InputTableInfo inputTableInfo = new InputTableInfo();
      inputTableInfo.setTable(tableName, rowFilter);
      
      return new InputTableInfo[]{inputTableInfo};
    }
  }
  
  static class TeraScanMap implements Mapper<WritableComparable, Writable, WritableComparable, Writable> {
    int count = 0;
    public void map(WritableComparable key, Writable value, 
        OutputCollector<WritableComparable, Writable> collector, 
        Reporter reporter) throws IOException {
      count++;
    }

    public void configure(JobConf jobConf) {
    }

    public void close() throws IOException {
      LOG.info("Scaned count:" + count);
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(conf);
      try {
        InputStream in = fs.open(new Path("/user/grid/khj/build.xml"));
        byte[] b = new byte[100];
        in.read(b);
        LOG.info("Read:" + new String(b));
        in.close();
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
      }
    }
  }
  
  public static void main(String[] args) throws IOException {
    if(args.length < 1) {
      System.out.println("Usage: java TeraScanJob <table name>");
      System.exit(0);
    }
    
    TeraScanJob job = new TeraScanJob();
    job.runJob(args[0]);
  }
}
