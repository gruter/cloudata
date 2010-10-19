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
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

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
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.parallel.hadoop.AbstractTabletInputFormat;
import org.cloudata.core.parallel.hadoop.CloudataMapReduceUtil;
import org.cloudata.core.parallel.hadoop.SimpleInputFormat;
import org.cloudata.core.tablet.TableSchema;
import org.cloudata.core.tablet.TableSchema.ColumnInfo;


public class TeraJob {
  public static final Log LOG = LogFactory.getLog(TeraJob.class.getName()); 
  
  public static void main(String[] args) throws IOException {
    if(args.length < 5) {
      System.out.println("Usage: java TeraJob <table name> <# map> <row data length> <total data(GB)> <keyOutputPath>");
      System.exit(0);
    }
    TeraJob job = new TeraJob();
    job.runJob(args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2]), Integer.parseInt(args[3]), args[4]);
  }
  
  public void runJob(String tableName, int numOfTablets, int dataLength, int totalGb, String keyOutputPath) throws IOException {
    CloudataConf nconf = new CloudataConf();
    
    JobConf jobConf = new JobConf(TeraJob.class);
    jobConf.set("user.name", nconf.getUserId());
    String libDir = CloudataMapReduceUtil.initMapReduce(jobConf);
    
    
    if(!CTable.existsTable(nconf, tableName)) {
      TableSchema tableInfo = new TableSchema(tableName, "Test");
      tableInfo.addColumn(new ColumnInfo("Col1"));
      tableInfo.addColumn(new ColumnInfo("Col2", TableSchema.CACHE_TYPE));
      tableInfo.addColumn(new ColumnInfo("Col3"));
      CTable.createTable(nconf, tableInfo);
    }
    jobConf.setJobName("TeraOnlineJob" + "(" + new Date() + ")");
    
    long rowsPerTask = ((((long)totalGb) * 1024L * 1024L * 1024L)/((long)dataLength)) / (long)numOfTablets;
    
    jobConf.setInt("teraJob.dataLength", dataLength);
    jobConf.setLong("teraJob.rowsPerTask", rowsPerTask);
    
    jobConf.setLong("mapred.task.timeout", 30 * 60 * 1000);
    
    FileOutputFormat.setOutputPath(jobConf, new Path(keyOutputPath));

    //<MAP>
    jobConf.setMapperClass(TeraOnlineMap.class);
    jobConf.setInputFormat(SimpleInputFormat.class);
    jobConf.set(AbstractTabletInputFormat.OUTPUT_TABLE, tableName);
    jobConf.setNumMapTasks(numOfTablets);
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
      CloudataMapReduceUtil.clearMapReduce(libDir);
    }
  }
  
  static class TeraOnlineMap implements Mapper<WritableComparable, Writable, WritableComparable, Writable> {
    CTable ctable;
    CloudataConf conf;
    IOException err;
    int taskNum;
    long rowsPerTask;
    byte[] data;
    int count = 0;
    TimeCheckThread timeCheckThread;
    Random rand = new Random();
    
    SimpleDateFormat dateFormat = new SimpleDateFormat("MM-dd HH:mm:ss.SSS");
    
    public void map(WritableComparable key, Writable value, 
        OutputCollector<WritableComparable, Writable> collector, 
        Reporter reporter) throws IOException {
      LOG.info("Start Map");
      if(err != null) {
        throw err;
      }

      DecimalFormat df = new DecimalFormat("00000000000000000000");
      
      //입력할 Tablet 추가
      //Row.Key endRowKey = new Row.Key(df.format((long)taskNum * rowsPerTask + rowsPerTask - 1L));
      //ctable.addTablet(endRowKey);
      
      timeCheckThread = new TimeCheckThread();
      timeCheckThread.start();
      for(long i = 0; i < rowsPerTask; i++) {
        long randNum = rand.nextLong();
        //Row.Key rowKey = new Row.Key(df.format((long)taskNum * rowsPerTask + i));
        Row.Key rowKey = new Row.Key(df.format(randNum));
        Row row = new Row(rowKey);
        row.addCell("Col1", new Cell(Cell.Key.EMPTY_KEY, this.data));
        row.addCell("Col2", new Cell(Cell.Key.EMPTY_KEY, "Col2_Data".getBytes()));
        row.addCell("Col3", new Cell(Cell.Key.EMPTY_KEY, "Col3_Data".getBytes()));
        timeCheckThread.touch(rowKey);
        ctable.put(row);
        collector.collect(new Text(rowKey.toString()), new Text(dateFormat.format(new Date())));
        if(i % 1000 == 0) {
          reporter.progress();
        }
        
        if( i % 10000 == 0) {
          LOG.info("uploaded: " + i);
        }
        count++;
      }
      
      LOG.info("End Map");
    }

    public void configure(JobConf jobConf) {
      String taskId = jobConf.get("mapred.task.id");
      String taskId2 = taskId.substring(taskId.length() - 8);
      this.rowsPerTask = jobConf.getLong("teraJob.rowsPerTask", 0L);
      this.taskNum = Integer.parseInt(taskId2.substring(0,6));
      
      Random rand = new Random();
      this.data = new byte [jobConf.getInt("teraJob.dataLength", 1000)];
      rand.nextBytes(data);
      
      try {
        this.conf = new CloudataConf(jobConf);
        System.out.println("User:" + jobConf.get("user.name") + "," + conf.getUserId());
        this.ctable = CTable.openTable(conf, jobConf.get(AbstractTabletInputFormat.OUTPUT_TABLE));
      } catch (IOException e) {
        this.err = e;
      }
    }

    public void close() throws IOException {
      if(timeCheckThread != null) {
        timeCheckThread.interrupt();
      }
    }

    
    static class TimeCheckThread extends Thread {
      AtomicLong lastTouchTime = new AtomicLong();
      Row.Key rowKey;
      public TimeCheckThread() {
        lastTouchTime.set(System.currentTimeMillis());
      }
      
      public void touch(Row.Key rowKey) {
        this.rowKey = rowKey;
        lastTouchTime.getAndSet(System.currentTimeMillis());
      }
      public void run() {
        while(true) {
          try {
            Thread.sleep(5 * 1000);
          } catch (InterruptedException e) {
            return;
          }
          
          long lastTime = lastTouchTime.get();
          long gap = System.currentTimeMillis() - lastTime;
          if( gap >  5 * 1000) {
            LOG.warn("time=" + gap + ",lastTime=" + lastTime + ",rowKey=" + rowKey);
          }
        }
      }
    }
  }
}
