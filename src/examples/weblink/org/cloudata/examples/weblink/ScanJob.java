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
package org.cloudata.examples.weblink;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.parallel.hadoop.AbstractTabletInputFormat;
import org.cloudata.core.parallel.hadoop.DefaultTabletInputFormat;
import org.cloudata.core.parallel.hadoop.CloudataMapReduceUtil;


/**
 * @author jindolk
 * 
 */
public class ScanJob {
  public static void main(String[] args) throws IOException {
    (new ScanJob()).run(args);
  }

  public void run(String[] args) throws IOException {
    if (args.length < 1) {
      System.out.println("Usage: java ScanJob <table name>");
      System.exit(0);
    }

    String tableName = args[0];

    CloudataConf nconf = new CloudataConf();
    if (!CTable.existsTable(nconf, tableName)) {
      System.out.println("No table: " + tableName);
      System.exit(0);
    }

    JobConf jobConf = new JobConf(UploadJob.class);
    jobConf.setJobName("CloudataExamles.weblink.ScanJob_" + new Date());
    String libDir = CloudataMapReduceUtil.initMapReduce(jobConf);

    // <MAP>
    jobConf.setInputFormat(DefaultTabletInputFormat.class);
    jobConf.setMapperClass(ScanJobMapper.class);
    jobConf.setMapOutputKeyClass(Text.class);
    jobConf.setMapOutputValueClass(Text.class);
    jobConf.set(AbstractTabletInputFormat.INPUT_TABLE, tableName);
    jobConf.set(AbstractTabletInputFormat.INPUT_COLUMN_LIST, "url");
    // </MAP>

    // <REDUCE>
    FileOutputFormat.setOutputPath(jobConf, new Path(
        "CloudataExamles_WebScanJob_" + System.currentTimeMillis()));
    jobConf.setReducerClass(ScanJobReducer.class);
    jobConf.setNumReduceTasks(1);
    // </REDUCE>

    try {
      JobClient.runJob(jobConf);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      CloudataMapReduceUtil.clearMapReduce(libDir);
    }
  }

  static class ScanJobMapper implements Mapper<Row.Key, Row, Text, Text> {

    static Log LOG = LogFactory.getLog(ScanJobMapper.class.getName());

    OutputCollector<Text, Text> output;

    int count = 0;

    String taskId;

    @Override
    public void map(Row.Key key, Row value, OutputCollector<Text, Text> output,
        Reporter reporter) throws IOException {
      if (this.output == null) {
        this.output = output;
      }
      count++;
    }

    public void configure(JobConf job) {
      taskId = job.get("mapred.task.id");
    }

    @Override
    public void close() throws IOException {
      output.collect(new Text(taskId), new Text(String.valueOf(count)));
    }
  }

  static class ScanJobReducer implements Reducer<Text, Text, Text, Text> {
    static Log LOG = LogFactory.getLog(ScanJobReducer.class.getName());

    OutputCollector<Text, Text> output;

    long count = 0;

    @Override
    public void reduce(Text key, Iterator<Text> values,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
      if (this.output == null) {
        this.output = output;
      }
      while (values.hasNext()) {
        count += Integer.parseInt(values.next().toString());
      }
    }

    public void configure(JobConf job) {
    }

    @Override
    public void close() throws IOException {
      output.collect(new Text("Result"), new Text(String.valueOf(count)));
    }
  }
}
