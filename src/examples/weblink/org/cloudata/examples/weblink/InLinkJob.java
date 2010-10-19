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
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.fs.CloudataFileSystem;
import org.cloudata.core.fs.GPath;
import org.cloudata.core.parallel.hadoop.CloudataMapReduceUtil;
import org.cloudata.core.parallel.hadoop.DefaultTabletInputFormat;
import org.cloudata.core.tablet.TableSchema;


public class InLinkJob {
  static Log LOG = LogFactory.getLog(InLinkJob.class.getName());
  
  public static void main(String[] args) throws IOException {
    if(args.length < 2) {
      System.out.println("Usage: java InLinkJob <input table> <output table>");
      System.exit(0);
    }
    
    String inputTableName = args[0];
    String outputTableName = args[1];
    
    CloudataConf nconf = new CloudataConf();
    if(!CTable.existsTable(nconf, outputTableName)) {
      TableSchema tableSchema = new TableSchema(outputTableName);
      tableSchema.addColumn("inlink");
      
      CTable.createTable(nconf, tableSchema);
    }
    
    Path tempPath = new Path("InLinkJob_tmp" + System.currentTimeMillis());
    
    JobConf jobConf = new JobConf(InLinkJob.class);
    String libDir = CloudataMapReduceUtil.initMapReduce(jobConf);
    
    //<Map>
    jobConf.setMapperClass(InLinkMap.class);
    jobConf.setInputFormat(DefaultTabletInputFormat.class);
    jobConf.set(DefaultTabletInputFormat.INPUT_TABLE, inputTableName);
    jobConf.set(DefaultTabletInputFormat.INPUT_COLUMN_LIST, "outlink");
    jobConf.setMapOutputKeyClass(Text.class);
    jobConf.setMapOutputValueClass(TextArray.class);
    //</Map>

    //<Reduce>
    FileOutputFormat.setOutputPath(jobConf, tempPath);
    jobConf.setReducerClass(InLinkReduce.class);
    jobConf.set(DefaultTabletInputFormat.OUTPUT_TABLE, outputTableName);
    //</Reduce>
    
    try {
      JobClient.runJob(jobConf);
    } finally {
      CloudataFileSystem fs = CloudataFileSystem.get(nconf);
      fs.delete(new GPath(tempPath.toString()), true);
      CloudataMapReduceUtil.clearMapReduce(libDir);
    }
  }
  
  static class InLinkMap implements Mapper<Row.Key, Row, Text, TextArray> {
    @Override
    public void map(Row.Key key, Row value, OutputCollector<Text, TextArray> output,
        Reporter reporter) throws IOException {
      //outlink.hashCode -> row.outlink.anchor_text, rowkey
      
      List<Cell> outlinks = value.getFirstColumnCells();
      if(outlinks == null || outlinks.size() == 0) {
        return;
      }
      
      for(Cell eachLink: outlinks) {
        output.collect(new Text(eachLink.getKey().toString()), 
            new TextArray(
                new Text[]{new Text(eachLink.getValue().toString()), new Text(key.toString())}));
      }
    }

    @Override
    public void configure(JobConf job) {
    }

    @Override
    public void close() throws IOException {
    }
  }
  
  static class InLinkReduce implements Reducer<Text, TextArray, Text, Text> {
    private CTable ctable;
    private IOException exception;
    @Override
    public void reduce(Text key, Iterator<TextArray> values,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
      if(exception != null) {
        throw exception;
      }
      Row.Key rowKey = new Row.Key(key.toString());
      
      Row row = new Row(rowKey);
      int count = 0;
      
      long timestamp = System.currentTimeMillis();
      while(values.hasNext()) {
        TextArray value = values.next();
        
        String anchorText = value.get(0).toString();
        String inlink = value.get(1).toString();
        
        row.addCell("inlink", new Cell(new Cell.Key(anchorText), inlink.getBytes(), timestamp));
        timestamp++;
        count++;
        
        if(count > 100) {
          ctable.put(row, false);
          row = new Row(rowKey);
          count = 0;
        }
      }
      
      if(count > 0) {
        ctable.put(row, false);
      }
    }

    @Override
    public void configure(JobConf job) {
      try {
        ctable = CTable.openTable(new CloudataConf(), 
            DefaultTabletInputFormat.OUTPUT_TABLE);
      } catch (IOException e) {
        exception = e;
      }
    }

    @Override
    public void close() throws IOException {
    }
  }
}
