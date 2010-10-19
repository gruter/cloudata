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
package org.cloudata.core.testjob.performance;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

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
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.util.FileUtil;
import org.cloudata.core.parallel.hadoop.CloudataMapReduceUtil;
import org.cloudata.core.parallel.hadoop.SimpleInputFormat;
import org.cloudata.core.tablet.TableSchema;
import org.cloudata.core.testjob.tera.TeraReadJob;


/**
 * @author jindolk
 *
 */
public class ManyTableJob {
  public static final Log LOG = LogFactory.getLog(ManyTableJob.class.getName()); 

  static int numOfTables = 2000;
  static List<TableCreationThread> threads = new ArrayList<TableCreationThread>();
  
  public static void main(String[] args) throws IOException {
    CloudataConf conf = new CloudataConf();
    if(!CTable.existsTable(conf, "T_MANY_TABLE_0")) {
      createTable(conf);
    }
    
    Path keyPath = putData();
    getData(conf, keyPath);
  }
  
  public static void getData(CloudataConf conf, Path keyPath) throws IOException {
    JobConf jobConf = new JobConf(TeraReadJob.class);
    jobConf.set("user.name", conf.getUserId());
    String libDir = CloudataMapReduceUtil.initMapReduce(jobConf);
    
    Path tempOutputPath = new Path("ManyTableJob_Get_" + System.currentTimeMillis());
    
    jobConf.setJobName("ManyTableJob_Get_" + "(" + new Date() + ")");
    
    TextOutputFormat.setOutputPath(jobConf, tempOutputPath);
    //<MAP>
    jobConf.setMapperClass(ManyTableGetMap.class);
    jobConf.setInputFormat(TextInputFormat.class);
    TextInputFormat.addInputPath(jobConf, keyPath);
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
  
  static class ManyTableGetMap implements Mapper<WritableComparable, Writable, WritableComparable, Writable> {
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
      
      if(this.ctable == null) {
        this.ctable = CTable.openTable(conf, valueStr);
        if(this.ctable == null) {
          throw new IOException("No table:" + valueStr);
        }
        return;
      }

      Row.Key rowKey = new Row.Key(valueStr);
      Row row = ctable.get(rowKey);
      if(row == null) {
        throw new IOException("No Data:[" + rowKey + "]");
      }
      Cell cell = row.getFirst("Col1");
      if(cell == null) {
        throw new IOException("No Cell Data:[" + rowKey + "]");
      } 
      if(count % 10000 == 0) {
        LOG.info("read: " + count + ", last row key[" + rowKey + "]");
      }
      count++;
    }

    public void configure(JobConf jobConf) {
      this.conf = new CloudataConf(jobConf);
      System.out.println("User:" + jobConf.get("user.name") + "," + conf.getUserId());
    }

    public void close() throws IOException {
      LOG.info("total read: " + count);
    }
  }
  
  public static Path putData() throws IOException {
    CloudataConf nconf = new CloudataConf();
    
    JobConf jobConf = new JobConf(ManyTableJob.class);
    jobConf.set("user.name", nconf.getUserId());
    String libDir = CloudataMapReduceUtil.initMapReduce(jobConf);
    
    jobConf.setJobName("ManyTableJob_Put" + "(" + new Date() + ")");
    
    jobConf.setLong("mapred.task.timeout", 30 * 60 * 1000);
    
    Path outputPath = new Path("ManyTableJob_KEY_" + System.currentTimeMillis());
    
    FileOutputFormat.setOutputPath(jobConf, outputPath);

    //<MAP>
    jobConf.setMapperClass(ManyTablePutMap.class);
    jobConf.setInputFormat(SimpleInputFormat.class);
    jobConf.setNumMapTasks(numOfTables);
    jobConf.setMapSpeculativeExecution(false);
    jobConf.setMaxMapAttempts(0);
    //</MAP>

    //<REDUCE>
    jobConf.setNumReduceTasks(0);
    //</REDUCE>
    
    try {
      //Run Job
      JobClient.runJob(jobConf);
      return outputPath;
    } finally {
      //delete temp output path
      FileSystem fs = FileSystem.get(jobConf);
      CloudataMapReduceUtil.clearMapReduce(libDir);
    }
  }
  
  static class ManyTablePutMap implements Mapper<WritableComparable, Writable, WritableComparable, Writable> {
    CTable ctable;
    CloudataConf conf;
    IOException err;
    byte[] data;
    int count = 0;
    Random rand = new Random();
    String tableName;
    
    public void map(WritableComparable key, Writable value, 
        OutputCollector<WritableComparable, Writable> collector, 
        Reporter reporter) throws IOException {
      LOG.info("Start Map");
      if(err != null) {
        throw err;
      }

      DecimalFormat df = new DecimalFormat("00000000000000000000");
      
      collector.collect(new Text(tableName), new Text(""));
      
      for(long i = 0; i < 50000; i++) {
        long randNum = rand.nextLong();
        Row.Key rowKey = new Row.Key(df.format(randNum));
        Row row = new Row(rowKey);
        row.addCell("Col1", new Cell(Cell.Key.EMPTY_KEY, this.data));
        ctable.put(row);
        if(i % 1000 == 0) {
          reporter.progress();
        }
        
        if( i % 10000 == 0) {
          LOG.info("uploaded: " + i);
        }
        collector.collect(new Text(df.format(randNum)), new Text(""));
      }
      
      LOG.info("End Map");
    }

    public void configure(JobConf jobConf) {
      tableName = "T_MANY_TABLE_" + CloudataMapReduceUtil.getMapNum(jobConf);
      
      Random rand = new Random();
      this.data = new byte [10000];
      rand.nextBytes(data);
      
      try {
        this.conf = new CloudataConf(jobConf);
        LOG.info("TableName:" + tableName);
        this.ctable = CTable.openTable(conf, tableName);
        if(ctable == null) {
          throw new IOException("No table:" + tableName);
        }
      } catch (IOException e) {
        this.err = e;
      }
    }

    public void close() throws IOException {
    }
  }
  
  private static void createTable(CloudataConf conf) throws IOException { 
    long startTime = System.currentTimeMillis();
    
    int threadNum = 50;
    
    synchronized(threads) {
      for(int i = 0; i < threadNum; i++) {
        TableCreationThread thread = new TableCreationThread(conf, i * numOfTables/threadNum, (i + 1) * numOfTables/threadNum);
        thread.start();
        threads.add(thread);
      }
    }

    boolean end = false;
    while(!end) {
      synchronized(threads) {
        end = true;
        for(TableCreationThread eachThread: threads) {
          if(eachThread.error != null) {
            throw new IOException(eachThread.error);
          }
          if(eachThread.isAlive()) {
            end = false;
            break;
          }
        }
        if(end) {
          break;
        }
        
        try {
          threads.wait();
        } catch (InterruptedException e) {
        }
      }
    }
    long endTime = System.currentTimeMillis();
    System.out.println(numOfTables + " created, " + (endTime - startTime) + " ms elapsed");
    
    startTime = System.currentTimeMillis();
    TableSchema[] tables = CTable.listTables(conf);
    endTime = System.currentTimeMillis();
    System.out.println(tables.length + " tables listing, " + (endTime - startTime) + " ms elapsed");
  }
  
  static class TableCreationThread extends Thread {
    int start;
    int end;
    CloudataConf conf;
    Exception error;
    
    public TableCreationThread(CloudataConf conf, int start, int end) {
      this.start = start;
      this.end = end;
      this.conf = conf;
    }
    
    public void run() {
      try {
        long startTime = System.currentTimeMillis();
        for(int i = start; i < end; i++) {
          TableSchema tableSchema = new TableSchema("T_MANY_TABLE_" + i);
          tableSchema.addColumn("Col1");
          tableSchema.addColumn("Col2");
          tableSchema.addColumn("Col3");
          tableSchema.addColumn("Col4");
          tableSchema.addColumn("Col5");
          tableSchema.addColumn("Col6");
          
          CTable.createTable(conf, tableSchema);
        }
        long endTime = System.currentTimeMillis();
        System.out.println((end - start) + " created, " + (endTime - startTime) + " ms elapsed");
        
      } catch (Exception e) {
        error = e;
        return;
      } finally {
        synchronized(threads) {
          threads.notifyAll();
        }
      }
    }
  }
}
