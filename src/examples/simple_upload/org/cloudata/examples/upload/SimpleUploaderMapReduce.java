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
package org.cloudata.examples.upload;

import java.io.IOException;
import java.util.Iterator;

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
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.DirectUploader;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.parallel.hadoop.AbstractTabletInputFormat;
import org.cloudata.core.parallel.hadoop.KeyRangePartitioner;
import org.cloudata.core.parallel.hadoop.CloudataMapReduceUtil;
import org.cloudata.core.tablet.TableSchema;
import org.cloudata.core.tablet.TabletInfo;
import org.cloudata.examples.first.HdfsToCloudataMapReduce;


/**
 * @author jindolk
 *
 */
public class SimpleUploaderMapReduce {
  public static void main(String[] args) throws IOException {
    (new SimpleUploaderMapReduce()).run(args);
 }
 
 public void run(String[] args) throws IOException {
   if (args.length < 3) {
     System.out.println("Usage: java SimpleUploaderMapReduce <input path> <table name> <# reduce>");
     System.exit(0);
   }

   Path inputPath = new Path(args[0]);
   String tableName = args[1];

   CloudataConf nconf = new CloudataConf();
   if (!CTable.existsTable(nconf, tableName)) {
     TableSchema tableSchema = new TableSchema(tableName);
     tableSchema.addColumn("Col1");

     Row.Key[] rowKeys = new Row.Key[20];
     for(int i = 0; i < 10; i ++) {
       rowKeys[i] = new Row.Key("-0" + i);
     }
     for(int i = 1; i < 10; i ++) {
       rowKeys[9 + i] = new Row.Key("0" + i);
     }
     rowKeys[19] = Row.Key.MAX_KEY;
     
     CTable.createTable(nconf, tableSchema, rowKeys);
   }
   JobConf jobConf = new JobConf(HdfsToCloudataMapReduce.class);
   String libDir = CloudataMapReduceUtil.initMapReduce(jobConf);

   // <MAP>
   FileInputFormat.addInputPath(jobConf, inputPath);
   jobConf.setInputFormat(TextInputFormat.class);
   jobConf.setMapperClass(SimpleUploaderMapper.class);
   jobConf.setPartitionerClass(KeyRangePartitioner.class);
   jobConf.setMapOutputKeyClass(Text.class);
   jobConf.setMapOutputValueClass(Text.class);
   jobConf.set(AbstractTabletInputFormat.OUTPUT_TABLE, tableName);
   // </MAP>

   // <REDUCE>
   FileOutputFormat.setOutputPath(jobConf, new Path("SimpleUploaderMapReduce_" + System.currentTimeMillis()));
   jobConf.setReducerClass(SimpleUploaderReducer.class);
   jobConf.setNumReduceTasks(Integer.parseInt(args[2]));
   jobConf.setMaxReduceAttempts(0);
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

 static class SimpleUploaderReducer implements Reducer<Text, Text, Text, Text> {

   static Log LOG = LogFactory.getLog(SimpleUploaderReducer.class.getName());

   private CTable ctable;

   private IOException err;

   private int count = 0;
   
   private DirectUploader uploader;
   
   @Override
   public void reduce(Text key, Iterator<Text> values,
       OutputCollector<Text, Text> output, Reporter reporter)
       throws IOException {
     if(err != null) {
       throw err;
     }
     Row.Key rowKey = new Row.Key(key.toString());
     while(values.hasNext()) {
       Text value = values.next();
       Row row = new Row(rowKey);
       row.addCell("Col1", new Cell(Cell.Key.EMPTY_KEY, value.toString().getBytes()));
       uploader.put(row);
       count++;
       if(count % 10000 == 0) {
         LOG.info(count + " inserted");
       }
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
       uploader = ctable.openDirectUploader(new String[]{"Col1"}, true);
     } catch (Exception e) {
       err = new IOException(e.getMessage());
       err.initCause(e);
     }
   }

   @Override
   public void close() throws IOException {
     try {
       uploader.close();
     } catch (IOException e) {
       uploader.rollback();
       throw e;
     }
     LOG.info(count + " inserted");
   }
 }
 
 static class SimpleUploaderMapper implements Mapper<LongWritable, Text, Text, Text> {
   static Log LOG = LogFactory.getLog(SimpleUploaderMapper.class.getName());

   int count = 0;
   @Override
   public void map(LongWritable key, Text value,
       OutputCollector<Text, Text> output, Reporter reporter)
       throws IOException {
     output.collect(value, new Text(String.valueOf(count)));
     count++;
//     if(count % 10000 == 0) {
//       LOG.info("KEY:" + value.toString());
//     }
   }

   @Override
   public void configure(JobConf job) {
   }

   @Override
   public void close() throws IOException {
   }
 }
}
