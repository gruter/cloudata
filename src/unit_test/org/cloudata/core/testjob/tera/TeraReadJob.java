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
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.util.FileUtil;
import org.cloudata.core.parallel.hadoop.CloudataMapReduceUtil;


/**
 * @author jindolk
 *
 */
public class TeraReadJob {
  public static final Log LOG = LogFactory.getLog(TeraReadJob.class.getName()); 
  
  public static void main(String[] args) throws IOException {
    if(args.length < 2) {
      System.out.println("Usage: java TeraReadJob <table name> <keyOutputPath>");
      System.exit(0);
    }
    
    String tableName = args[0];
    String keyOutputPath = args[1];
    
    CloudataConf nconf = new CloudataConf();
    
    JobConf jobConf = new JobConf(TeraReadJob.class);
    jobConf.set("user.name", nconf.getUserId());
    String libDir = CloudataMapReduceUtil.initMapReduce(jobConf);
    
    if(!CTable.existsTable(nconf, tableName)) {
      System.out.println("Error: No table " + tableName);
      System.exit(0);
    }
    Path tempOutputPath = new Path("TeraReadJob" + System.currentTimeMillis());
    
    jobConf.setJobName("TeraReadJob" + "(" + new Date() + ")");
    jobConf.set("TeraReadJob.tableName", tableName);
    
    TextOutputFormat.setOutputPath(jobConf, tempOutputPath);
    //<MAP>
    jobConf.setMapperClass(TeraReadMap.class);
    jobConf.setInputFormat(TextInputFormat.class);
    TextInputFormat.addInputPath(jobConf, new Path(keyOutputPath));
    jobConf.setMapSpeculativeExecution(false);
    jobConf.setMaxMapAttempts(0);
    //</MAP>

    //<REDUCE>
    jobConf.setNumReduceTasks(0);
    //</REDUCE>
    
    try {
      //Run Job
      JobClient.runJob(jobConf);
    } finally {
      //delete temp output path
      FileSystem fs = FileSystem.get(jobConf);
      FileUtil.delete(fs, tempOutputPath, true);
      CloudataMapReduceUtil.clearMapReduce(libDir);
    }
  } 
  
  static class TeraReadMap implements Mapper<WritableComparable, Writable, WritableComparable, Writable> {
    CTable ctable;
    CloudataConf conf;
    IOException err;
    int count = 1;
    
    public void map(WritableComparable key, Writable value, 
        OutputCollector<WritableComparable, Writable> collector, 
        Reporter reporter) throws IOException {
      if(err != null) {
        throw err;
      }

      String valueStr = value.toString().trim();
      if(valueStr.indexOf("\t") > 0) {
        valueStr = valueStr.substring(0, valueStr.indexOf("\t"));
      }
      Row.Key rowKey = new Row.Key(valueStr);
      Row row = ctable.get(rowKey);
      if(row == null) {
        throw new IOException("No Data:[" + rowKey + "]");
      }
      Cell cell = row.getFirst("Col1");
      if(cell == null) {
        throw new IOException("No Cell Data:[" + rowKey + "].Col1");
      } 
      cell = row.getFirst("Col2");
      if(cell == null) {
        throw new IOException("No Cell Data:[" + rowKey + "].Col2");
      } 
      if(count % 10000 == 0) {
        LOG.info("read: " + count + ", last row key[" + rowKey + "]");
      }
      count++;
    }

    public void configure(JobConf jobConf) {
      try {
        this.conf = new CloudataConf(jobConf);
        System.out.println("User:" + jobConf.get("user.name") + "," + conf.getUserId());
        this.ctable = CTable.openTable(conf, jobConf.get("TeraReadJob.tableName"));
        if(ctable == null) {
          throw new IOException("No table:" + jobConf.get("TeraReadJob.tableName"));
        }
      } catch (IOException e) {
        this.err = e;
      }
    }

    public void close() throws IOException {
      LOG.info("total read: " + count);
    }
  }
}
