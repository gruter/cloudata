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
package org.cloudata.util.upload;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.cloudata.core.client.CTable;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.parallel.hadoop.AbstractTabletInputFormat;


/**
 * @author jindolk
 *
 */
class UploadMap implements Mapper<LongWritable, Text, Text, Text> {
  private CTable ctable;
  private IOException err;
  private String delim;
  private String[] columns;
  private int[] fieldNums;
  private boolean keyValuePair;
  
  private long timestamp = System.currentTimeMillis();
  
  private int count = 0;
  
  @Override
  public void map(LongWritable key, Text value,
      OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
    if(err != null) {
      throw err;
    }
    String line = value.toString();
    UploadUtil.putToTable(ctable, line, delim, columns, fieldNums, keyValuePair, timestamp);
    count++;
    if(count % 5000 == 0) {
      System.out.println(count + " rows inserted");
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
      delim = job.get("uploadJob.delim", "\t");
      columns = job.get("uploadJob.columns").split(",");
      
      String[] fieldNumStr = job.get("uploadJob.fieldNums").split(",");
      fieldNums = new int[fieldNumStr.length];
      for(int i = 0; i < fieldNumStr.length; i++) {
        fieldNums[i] = Integer.parseInt(fieldNumStr[i]);
      }
      
      keyValuePair = job.getBoolean("uploadJob.keyValuePair", false);
    } catch (Exception e) {
      err = new IOException(e.getMessage());
      err.initCause(e);
    }
  }

  @Override
  public void close() throws IOException {
  }
}
