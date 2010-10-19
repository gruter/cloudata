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

import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.parallel.hadoop.DefaultTabletInputFormat;
import org.cloudata.core.tablet.TableSchema;


public class TermGlobalJob {
  public static final Log LOG = LogFactory.getLog(TermGlobalJob.class.getName()); 
  public static final String GLOBAL_TABLE = "T_GLOBAL";
  public static final String[] GLOBAL_TABLE_COLUMNS = new String[]{"doclengh", "doccount"};
  public static final Row.Key GLOBAL_TABLE_ROWKEY = new Row.Key("WebGlobalVariable");
  
  public void exec() throws Exception {
    CloudataConf nconf = new CloudataConf();
    if(!CTable.existsTable(nconf, GLOBAL_TABLE)) {
      TableSchema globalTableInfo = new TableSchema(GLOBAL_TABLE, "Test", GLOBAL_TABLE_COLUMNS);
      CTable.createTable(nconf, globalTableInfo);
    }
    
    Path tempOutputPath = new Path("globalTableInfo" + System.currentTimeMillis());
    
    JobConf jobConf = new JobConf(WebTableJob.class);
    jobConf.setJobName("TermGlobalJob" + "(" + new Date() + ")");
    
    //<MAP>
    jobConf.setMapperClass(TermGlobalMap.class);
    jobConf.setInputFormat(DefaultTabletInputFormat.class);
    jobConf.set(DefaultTabletInputFormat.INPUT_TABLE, WebTableJob.WEB_TABLE);
    jobConf.set(DefaultTabletInputFormat.INPUT_COLUMN_LIST, 
                          WebTableJob.WEB_TABLE_COLUMNS[2]);
    jobConf.setMapOutputKeyClass(Text.class);
    jobConf.setMapOutputValueClass(Text.class);    
    //</MAP>

    //<REDUCE>
    jobConf.setReducerClass(TermGlobalReduce.class);
    jobConf.setOutputKeyClass(Text.class);
    jobConf.setOutputValueClass(Text.class);   
    jobConf.setNumReduceTasks(1);
    jobConf.setMaxReduceAttempts(0);
    FileOutputFormat.setOutputPath(jobConf, tempOutputPath);
    //</REDUCE>
    
    //Run Job
    JobClient.runJob(jobConf);
    
    //delete temp output path
    FileSystem fs = FileSystem.get(jobConf);
    fs.delete(tempOutputPath, true);
  }
  
  static class TermGlobalMap implements Mapper<Row.Key, Row, WritableComparable, Writable> {
    static final Log LOG = LogFactory.getLog(TermGlobalMap.class.getName());
    
    long sumDocLength;
    long sumDocCount;
    OutputCollector<WritableComparable, Writable> collector;
    
    public void map(Row.Key key, Row value, 
        OutputCollector<WritableComparable, Writable> collector, 
        Reporter reporter) throws IOException {
      if(this.collector == null) {
        this.collector = collector;
      }
      
      String document = new String(key.getBytes(), 0, key.getLength(), "EUC-KR");
      
      if(value.getColumnSize() < 1) {
        LOG.warn("Length of RowColumnValuesArray less than 1: " + document);
        return;
      }
      
      Collection<Cell> cells = value.getFirstColumnCells();
      if(cells == null) {
        LOG.warn("Contents column is empty : " + document);
        return;
      }
      
      //contents에 대해서 term으로 나눈 다음 term, document, freq를 write한다.
      byte[] contentsBytes = cells.iterator().next().getBytes();
      if(contentsBytes == null || contentsBytes.length == 0) {
        LOG.info("Contents is null: " + document);
        return;
      }
      sumDocLength += contentsBytes.length;
      sumDocCount++;
    }

    public void configure(JobConf jobConf) {
    }

    public void close() throws IOException {
      LOG.info("sumDocLength: " + sumDocLength + ",sumDocCount: " + sumDocCount);
      collector.collect(new Text(String.valueOf(System.currentTimeMillis())), 
          new Text(Long.toString(sumDocLength) + "\t" + 
              Long.toString(sumDocCount)));
    }  
  }
  
  static class TermGlobalReduce implements Reducer<WritableComparable, Writable, WritableComparable, Writable> {
    static final Log LOG = LogFactory.getLog(TermGlobalReduce.class.getName());
    
    long sumDocLength;
    long sumDocCount;
    
    public void reduce(WritableComparable key, 
        Iterator<Writable> values, 
        OutputCollector<WritableComparable, Writable> collector, 
        Reporter reporter) throws IOException {
      while(values.hasNext()) {
        String[] tokens = values.next().toString().split("\t");
        sumDocLength += Long.parseLong(tokens[0]);
        sumDocCount += Long.parseLong(tokens[1]);
      }
    }
    
    public void configure(JobConf jobConf) {
    }

    public void close() throws IOException {
      CTable table = CTable.openTable(new CloudataConf(), GLOBAL_TABLE);
      try {
        Row row = new Row(GLOBAL_TABLE_ROWKEY);
        row.addCell("doclengh", new Cell(Cell.Key.EMPTY_KEY, Long.toString(sumDocLength).getBytes()));
        row.addCell("doccount", new Cell(Cell.Key.EMPTY_KEY, Long.toString(sumDocCount).getBytes()));
        
        table.put(row);
      } catch(IOException e) {
        LOG.error(e);
      }
    } 
  }
}
