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
package org.cloudata.examples.first;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.parallel.hadoop.AbstractTabletInputFormat;
import org.cloudata.core.parallel.hadoop.CloudataMapReduceUtil;
import org.cloudata.core.tablet.TableSchema;


/**
 * @author jindolk
 *
 */
public class HdfsToCloudataMapReduce {
  public static void main(String[] args) throws IOException {
    (new HdfsToCloudataMapReduce()).run(args);
 }
 
 public void run(String[] args) throws IOException {
   if (args.length < 2) {
     System.out.println("Usage: java HdfsToCloudataMapReduce <input path> <table name>");
     System.exit(0);
   }

   Path inputPath = new Path(args[0]);
   String tableName = args[1];

   CloudataConf nconf = new CloudataConf();
   if (!CTable.existsTable(nconf, tableName)) {
     TableSchema tableSchema = new TableSchema(tableName);
     tableSchema.addColumn("col1");

     CTable.createTable(nconf, tableSchema);
   }

   JobConf jobConf = new JobConf(HdfsToCloudataMapReduce.class);
   String libDir = CloudataMapReduceUtil.initMapReduce(jobConf);

   // <MAP>
   FileInputFormat.addInputPath(jobConf, inputPath);
   jobConf.setInputFormat(TextInputFormat.class);
   jobConf.setMapperClass(HdfsToCloudataMappper.class);
   jobConf.set(AbstractTabletInputFormat.OUTPUT_TABLE, tableName);
   // </MAP>

   // <REDUCE>
   // Map Only
   FileOutputFormat.setOutputPath(jobConf, new Path("HdfsToCloudataMapReduce_" + System.currentTimeMillis()));
   jobConf.setNumReduceTasks(0);
   // </REDUCE>

   try {
     JobClient.runJob(jobConf);
   } catch (Exception e) {
     e.printStackTrace();
   } finally {
     FileSystem fs = FileSystem.get(jobConf);
     fs.delete(FileOutputFormat.getOutputPath(jobConf), true);
     CloudataMapReduceUtil.clearMapReduce(libDir);
   }
 }

 static class HdfsToCloudataMappper implements Mapper<LongWritable, Text, Text, Text> {
   static Log LOG = LogFactory.getLog(HdfsToCloudataMappper.class.getName());

   private CTable ctable;

   private IOException err;

   private int count = 0;
   
   @Override
   public void map(LongWritable key, Text value,
       OutputCollector<Text, Text> output, Reporter reporter)
       throws IOException {
     if(err != null) {
       throw err;
     }
     String record = value.toString();
     Row.Key rowKey = new Row.Key(record);
     Row row = new Row(rowKey);
     row.addCell("col1", new Cell(Cell.Key.EMPTY_KEY, "TestData".getBytes()));
     ctable.put(row);
     
     count++;
     if(count % 1000 == 0) {
       LOG.info(count + " inserted");
     }
   }

   @Override
   public void configure(JobConf job) {
     try {
       String tableName = job.get(AbstractTabletInputFormat.OUTPUT_TABLE);
       CloudataConf nconf = new CloudataConf(job);
       ctable = CTable.openTable(nconf, tableName);
       if(ctable == null) {
         throw new IOException("No table:" + tableName);
       }
     } catch (Exception e) {
       err = new IOException(e.getMessage());
       err.initCause(e);
     }
   }

   @Override
   public void close() throws IOException {
     LOG.info(count + " pages inserted");
   }
 }
}
