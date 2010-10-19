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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.fs.CloudataFileSystem;
import org.cloudata.core.fs.GPath;
import org.cloudata.core.parallel.hadoop.CloudataMapReduceUtil;
import org.cloudata.core.parallel.hadoop.SimpleInputFormat;


/**
 * @author jindolk
 *
 */
public class TestMultiThreadCTable {
  public static final Log LOG = LogFactory.getLog(ManyTableJob.class.getName()); 
  static List<GetThread> threads = new ArrayList<GetThread>();
  
  public static void main(String[] args) throws IOException {
    if(args.length < 2) {
      System.out.println("Usage: java TestMultiThreadNTable <put|get> <key path>");
      System.exit(0);
    }
    
    if("put".equals(args[0])) {
      putData(args[1]);
    } else {
      getData(args[1]);
    }
  }
  
  public static Path putData(String outputDir) throws IOException {
    CloudataConf nconf = new CloudataConf();
    
    JobConf jobConf = new JobConf(TestMultiThreadCTable.class);
    jobConf.set("user.name", nconf.getUserId());
    String libDir = CloudataMapReduceUtil.initMapReduce(jobConf);
    
    jobConf.setJobName("TestMultiThreadNTable_" + "(" + new Date() + ")");
    
    jobConf.setLong("mapred.task.timeout", 30 * 60 * 1000);
    
    Path outputPath = new Path(outputDir);
    
    FileOutputFormat.setOutputPath(jobConf, outputPath);

    JobClient jobClient = new JobClient();
    
    int numOfRowPerMap = 100000/jobClient.getClusterStatus().getMaxMapTasks();
    jobConf.setInt("numOfRowPerMap", numOfRowPerMap);
    //<MAP>
    jobConf.setMapperClass(PutDataMap.class);
    jobConf.setInputFormat(SimpleInputFormat.class);
    jobConf.setNumMapTasks(jobClient.getClusterStatus().getMaxMapTasks());
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
      FileSystem fs = FileSystem.get(jobConf);
      CloudataMapReduceUtil.clearMapReduce(libDir);
    }
  }
  
  static class PutDataMap implements Mapper<WritableComparable, Writable, WritableComparable, Writable> {
    CTable ctable;
    CloudataConf conf;
    IOException err;
    int count = 1;
    String tableName;
    byte[] data;
    
    int mapNum;
    int numOfRowPerMap;
    
    public void map(WritableComparable key, Writable value, 
        OutputCollector<WritableComparable, Writable> collector, 
        Reporter reporter) throws IOException {
      if(err != null) {
        throw err;
      }      
      DecimalFormat df = new DecimalFormat("00000000000000000000");
      
      collector.collect(new Text(tableName), new Text(""));
      
      for(long i = 0; i < numOfRowPerMap; i++) {
        Row.Key rowKey = new Row.Key(df.format(mapNum * numOfRowPerMap) + i);

        for(int j = 0; j < 90; j++) {
          Row row = new Row(rowKey);
          for(int k = 0; j < 50; k++) {
            Cell.Key cellKey = new Cell.Key(df.format(j * 90 + k));
            row.addCell("Col1", new Cell(cellKey, this.data));
          }
          
          ctable.put(row);
        }
        if(i % 10 == 0) {
          reporter.progress();
        }
        
        if( i % 100 == 0) {
          LOG.info("uploaded: " + i);
        }
        collector.collect(new Text(rowKey.toString()), new Text(""));
      }
    }

    public void configure(JobConf jobConf) {
      this.conf = new CloudataConf(jobConf);
      this.numOfRowPerMap = jobConf.getInt("numOfRowPerMap", 10000);
      this.mapNum = CloudataMapReduceUtil.getMapNum(jobConf);
      this.tableName = "T_MULTI_THREAD_" + mapNum;
      Random rand = new Random();
      this.data = new byte [200];
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
      LOG.info("total: " + count);
    }
  }
  
  public static void getData(String keyPath) throws IOException {
    CloudataConf conf = new CloudataConf();
    CloudataFileSystem fs = CloudataFileSystem.get(conf);
    
    GPath[] paths = fs.list(new GPath(keyPath));
    synchronized(threads) {
      for(GPath path: paths) {
        if(path.getName().indexOf("part") >= 0) {
          GetThread thread = new GetThread(fs, conf, path); 
          thread.start();
          threads.add(thread);
        }
      }
    }
    
    System.out.println("Started Get:# threads:" + threads.size());
    boolean end = false;
    while(!end) {
      synchronized(threads) {
        end = true;
        for(GetThread eachThread: threads) {
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
    
    System.out.println("End Get");
  }
  
  static class GetThread extends Thread {
    CloudataConf conf;
    GPath path;
    Exception error;
    CloudataFileSystem fs;
    public GetThread(CloudataFileSystem fs, CloudataConf conf, GPath path) {
      this.conf = conf;
      this.path = path;
      this.fs = fs;
    }
    
    public void run() {
      BufferedReader reader = null;
      try {
        reader = new BufferedReader(new InputStreamReader(fs.open(path)));
        String line = null;
        line = reader.readLine();
        if(line == null) {
          throw new IOException("No data in:" + path);
        }
        String valueStr = line.toString().trim();
        if(valueStr.indexOf("\t") > 0) {
          valueStr = valueStr.substring(0, valueStr.indexOf("\t"));
        }
        
        CTable ctable = CTable.openTable(conf, valueStr);
        if(ctable == null) {
          throw new IOException("No table:" + valueStr);
        }
        
        long startTime = System.currentTimeMillis();
        int count = 0;
        while( (line = reader.readLine()) != null) {
          if(valueStr.indexOf("\t") > 0) {
            valueStr = valueStr.substring(0, valueStr.indexOf("\t"));
          }
          
          Row row = ctable.get(new Row.Key(valueStr));
          if(row == null) {
            throw new IOException("No row:" + valueStr + "," + ctable.getTableName());
          }
          
          count ++;
          if(count % 1000 == 0) {
            System.out.println(ctable.getTableName() + ":" + count);
          }
        }
        long endTime = System.currentTimeMillis();
        
        System.out.println(ctable.getTableName() + ":" + count + "," + (endTime - startTime) + " ms");
      } catch (Exception e) {
        error = e;
      } finally {
        if(reader != null) {
          try {
            reader.close();
          } catch (IOException e) {
          }
        }
        synchronized(threads) {
          threads.notifyAll();
        }
      }
    }
  }
}
