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
package org.cloudata.examples.upload.partitionjob;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.util.FileUtil;
import org.cloudata.core.parallel.hadoop.AbstractTabletInputFormat;
import org.cloudata.core.parallel.hadoop.CloudataMapReduceUtil;
import org.cloudata.core.tablet.TableSchema;


public class PartitionJob {
  public static final Log LOG = LogFactory.getLog(PartitionJob.class.getName()); 
  public boolean runJob(String inputPath, String tableName, int numOfTablets) throws IOException {
    JobConf jobConf = new JobConf(PartitionJob.class);
    String libDir = CloudataMapReduceUtil.initMapReduce(jobConf);
    
    FileSystem fs = FileSystem.get(jobConf);
    //기존 파일 삭제
    FileUtil.delete(fs, new Path(getLogCountFilepath(tableName)), true);
    
    jobConf.setJobName("PartitionJob_" + tableName+ "(" + new Date() + ")");
    jobConf.set("cloudata.numOfTablets", String.valueOf(numOfTablets));
    jobConf.set(AbstractTabletInputFormat.OUTPUT_TABLE, tableName);
    
    String clientOpt = jobConf.get("mapred.child.java.opts");
    if(clientOpt ==  null) {
      clientOpt = "";
    }
    jobConf.set("mapred.child.java.opts", clientOpt + " -Duser.name=" + System.getProperty("user.name"));
    
    //<Map>
    FileInputFormat.addInputPath(jobConf, new Path(inputPath));
    jobConf.setInputFormat(TextInputFormat.class);
    jobConf.setMapperClass(PartitionMap.class);
    jobConf.setMapOutputKeyClass(Text.class);
    jobConf.setMapOutputValueClass(Text.class);
    //</Map>
    
    //<Reduce>
    Path tempOutputPath = new Path("temp/partitionJob/" + tableName + "/reducer");
    FileOutputFormat.setOutputPath(jobConf, tempOutputPath);
    jobConf.setOutputKeyClass(Text.class);
    jobConf.setOutputValueClass(Text.class);
    jobConf.setReducerClass(PartitionReducer.class);
    //Reduce 갯수는 1로 설정
    jobConf.setNumReduceTasks(1);
    //</Reduce>
    
    try {
      RunningJob job = JobClient.runJob(jobConf);
      return job.isSuccessful();
    } finally {
      FileUtil.delete(fs, new Path(getLogCountFilepath(tableName)), true);
      FileUtil.delete(fs, tempOutputPath, true);
      CloudataMapReduceUtil.clearMapReduce(libDir);
    }
  }
  
  public static String getLogCountFilepath(String tableName) throws IOException {
    return "temp/uploadJob/" + tableName + "/partition";
  }
  
  public static class PartitionReducer implements Reducer<WritableComparable, Writable, WritableComparable, Writable> {
    private JobConf jobConf;
    private boolean first = true;
    private int tabletCount;
    private int rangeKeyCount;
    private long keyIndex = 1;
    private List<Row.Key> rowKeys = new ArrayList<Row.Key>();
    private int totalKeyCountFromMapper;
    private String tableName;
    public void reduce(WritableComparable key, 
        Iterator<Writable> values, 
        OutputCollector<WritableComparable, Writable> collector, Reporter reporter) throws IOException {
      //Reduce의 configure()는 reduce가 시작될 때 실행된다.
      //reduce가 시작되는 시점은 map과 같이 시작되기 때문에
      //map의 결과를 이용하여 초기화 하는 기능을 configure()에 넣을 수 없기 때문에
      //별도의 init() 기능을 추가하였다.
      if(first) {
        first = false;
        init();
      }

      String keyValue = key.toString();
      if (keyIndex != 1 && keyIndex % rangeKeyCount == 0) {
        rowKeys.add(new Row.Key(keyValue));
      }
      keyIndex++;
    }

    private void init() throws IOException {
      //Map에서 write된 전체 key 개수를 계산한다. 
      Path countPath = new Path(getLogCountFilepath(tableName));
      LOG.info("Load rowkey count info:" + countPath);
      
      FileSystem fs = FileSystem.get(jobConf);
      FileStatus[] paths = fs.listStatus(countPath);
      if(paths == null || paths.length == 0) {
        throw new IOException("No key count info:" + countPath);
      }
      
      for(FileStatus eachPath: paths) {
        try {
          String pathName = eachPath.getPath().getName();
          if(pathName.indexOf("__") > 0) {
            totalKeyCountFromMapper += Integer.parseInt(pathName.substring(pathName.indexOf("__") + 2));
          } else {
            LOG.info("Wrong key count format:" + eachPath);
          }
        } catch (Exception e) {
          LOG.info("Wrong key count format:" + eachPath);
        }
      }
      
      //파티션할 tablet의 갯수
      tabletCount = jobConf.getInt("cloudata.numOfTablets", 10);
      rangeKeyCount = totalKeyCountFromMapper / tabletCount;    
      
      LOG.info("rangeKeyCount for partition: " + rangeKeyCount);
      if(rangeKeyCount == 0) {
        throw new IOException("Range Key count is 0");
      }
    }
    
    public void configure(JobConf jobConf) {
      this.jobConf = jobConf;
      this.tableName = jobConf.get(AbstractTabletInputFormat.OUTPUT_TABLE);
    }

    public void close() throws IOException {
      rowKeys.add(Row.Key.MAX_KEY);

      TableSchema tableInfo = new TableSchema(tableName);
      tableInfo.addColumn("title");
      tableInfo.addColumn("contents");
      
      //파티션 되어 있는 rowkey 정보를 이용하여 테이블을 생성한다.
      CloudataConf conf = new CloudataConf();
      if(!CTable.existsTable(conf, tableInfo.getTableName())) {
        try {
          CTable.createTable(conf, tableInfo, rowKeys.toArray(new Row.Key[rowKeys.size()]));
        } catch (Exception e) {
          //무시한다.
        }
      } 
      
      try {
        Thread.sleep(2 * 1000);
      } catch (InterruptedException e) {
      }
    }
  }
  
  public static class PartitionMap 
        implements Mapper<WritableComparable, Writable, WritableComparable, Writable> {
    private int keyCount;
    private int writeKeyCount;
    private String tableName;
    private JobConf jobConf;
    public void map(
        WritableComparable key, 
        Writable value, 
        OutputCollector<WritableComparable, Writable> collector,
        Reporter reporter) throws IOException {
      String record = value.toString();
      
      String recordKey = UploadJob.parseRecord(record)[0];
      
      keyCount++;
      //key의 분포를 이용해서 파티셔닝 하기 때문에 모든 레코드의 key를
      //reduce로 보낼 필요는 없다.
      //성능에 영향을 미치지 않을 만큼 조절한다.
      if(keyCount % 1000 == 0) {
        writeKeyCount++;
        collector.collect(new Text(recordKey), new Text(""));
      }
    }

    public void configure(JobConf jobConf) {
      tableName = jobConf.get(AbstractTabletInputFormat.OUTPUT_TABLE);
      this.jobConf = jobConf;
    }

    public void close() throws IOException {
      //HDFS에 map에서 write한 key의 갯수를 기록한다.
      //이 정보는 reduce에서 tablet의 rowkey 범위를 결정하는데 사용한다.
      String taskId = jobConf.get("mapred.task.id");
      
      FileSystem fs = FileSystem.get(jobConf);
      Path countPath = new Path(getLogCountFilepath(tableName));
      Path path = new Path(countPath, taskId + "__" + String.valueOf(writeKeyCount));
      
      LOG.info("create rowkey count path:" + path);
      boolean result = fs.mkdirs(path);
      if(!result) {
        LOG.error("Fail create rowkey count:" + path);
      }
    }
  }
}
