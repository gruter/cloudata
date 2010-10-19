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
import java.util.Date;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.hadoop.mapred.TextInputFormat;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.DirectUploader;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.util.FileUtil;
import org.cloudata.core.parallel.hadoop.AbstractTabletInputFormat;
import org.cloudata.core.parallel.hadoop.KeyRangePartitioner;
import org.cloudata.core.parallel.hadoop.CloudataMapReduceUtil;
import org.cloudata.core.tablet.TabletInfo;


public class UploadJob {
  public static final Log LOG = LogFactory.getLog(UploadJob.class.getName()); 
  
  public void runJob(String inputPath, String tableName) throws IOException {
    JobConf jobConf = new JobConf(UploadJob.class);
    String libDir = CloudataMapReduceUtil.initMapReduce(jobConf);
    
    jobConf.setJobName("UploadJob_" + tableName+ "(" + new Date() + ")");
    
    //KeyRangePartitioner를 사용하기 위해서는 반드시 
    //AbstractTabletInputFormat.OUTPUT_TABLE에 테이블명 지정
    jobConf.set(AbstractTabletInputFormat.OUTPUT_TABLE, tableName);
    
    CloudataConf conf = new CloudataConf();
    CTable ctable = CTable.openTable(conf, tableName);
    TabletInfo[] tabletInfos = ctable.listTabletInfos();
    
    //<Map>
    FileInputFormat.addInputPath(jobConf, new Path(inputPath));
    jobConf.setInputFormat(TextInputFormat.class);
    jobConf.setMapperClass(UploadMap.class);
    jobConf.setMapOutputKeyClass(Text.class);
    jobConf.setMapOutputValueClass(Text.class);
    jobConf.setMapSpeculativeExecution(false);
    jobConf.setMaxMapAttempts(0);
    jobConf.setPartitionerClass(KeyRangePartitioner.class);
    //</Map>
    
    //<Reduce>
    Path tempOutputPath = new Path("temp/uploadJob/" + tableName + "/reducer");
    FileOutputFormat.setOutputPath(jobConf, tempOutputPath);
    jobConf.setOutputKeyClass(Text.class);
    jobConf.setOutputValueClass(Text.class);
    jobConf.setReducerClass(UploadReducer.class);
    jobConf.setReduceSpeculativeExecution(false);
    jobConf.setMaxReduceAttempts(0);
    //Reduce 갯수는 Tablet갯수로 설정
    jobConf.setNumReduceTasks(tabletInfos.length);
    //</Reduce>
    
    try {
      JobClient.runJob(jobConf);
    } finally {
      FileSystem fs = FileSystem.get(jobConf);
      FileUtil.delete(fs, tempOutputPath, true);
      CloudataMapReduceUtil.clearMapReduce(libDir);
    }
  }
  
  public static String[] parseRecord(String record) {
    return record.split("\t");
  }
  
  public static class UploadReducer 
      implements Reducer<WritableComparable, Writable, WritableComparable, Writable> {
    private IOException confException;
    private DirectUploader uploader;
    public void reduce(WritableComparable key, 
        Iterator<Writable> values, 
        OutputCollector<WritableComparable, Writable> collector, 
        Reporter reporter) throws IOException {
      if(confException != null) {
        throw new IOException(confException.getMessage());
      }

      Row.Key rowKey = new Row.Key(key.toString());
      
      Row row = new Row(rowKey);
      while(values.hasNext()) {
        row.addCell("title", new Cell(Cell.Key.EMPTY_KEY, 
            values.next().toString().getBytes()));
      }
      uploader.put(row);
    }

    
    public void configure(JobConf jobConf) {
      String tableName = jobConf.get(AbstractTabletInputFormat.OUTPUT_TABLE);
      try {
        CloudataConf conf = new CloudataConf();
        CTable ctable = CTable.openTable(conf, tableName);
        uploader = ctable.openDirectUploader(new String[]{"title"});
      } catch (Exception e) {
        confException = new IOException(e.getMessage());
        confException.initCause(e);
      }
    }

    public void close() throws IOException {
      uploader.close();
    }
  }
  
  public static class UploadMap 
        implements Mapper<WritableComparable, Writable, WritableComparable, Writable> {
    public void map(
        WritableComparable key, 
        Writable value, 
        OutputCollector<WritableComparable, Writable> collector,
        Reporter reporter) throws IOException {
      String record = value.toString();
      
      String[] parsedData = UploadJob.parseRecord(record);
      
      //Key와 value로 분리해서 reduce로 보낸다.
      collector.collect(new Text(parsedData[0]), new Text(parsedData[1]));
    }

    public void configure(JobConf jobConf) {
    }

    public void close() throws IOException {
    }
  }
}
