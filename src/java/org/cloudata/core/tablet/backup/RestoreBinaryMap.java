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
package org.cloudata.core.tablet.backup;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.cloudata.core.client.DirectUploader;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.parallel.hadoop.DefaultTabletInputFormat;


/**
 * @author jindolk
 *
 */
public class RestoreBinaryMap implements Mapper<BytesWritable,BytesWritable, Text, Text> {
  static final Log LOG = LogFactory.getLog(RestoreBinaryMap.class.getName());
  DirectUploader uploader;
  IOException confErr;
  int count = 0;
  CsvParser parser = new CsvParser();

  @Override
  public void map(BytesWritable key, BytesWritable value,
      OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
    if(confErr != null) {
      throw confErr;
    }
    ByteArrayInputStream bin = new ByteArrayInputStream(key.get(), 0, key.getSize());
    DataInputStream in = new DataInputStream(bin);
    Row row = new Row();
    row.readFields(in);
    
    uploader.put(row);
    count++;
    if(count % 10000 == 0) {
      LOG.info(count + " cells restored");
    }
    in.close();
  }

  @Override
  public void configure(JobConf job) {
    try {
      CloudataConf nconf = new CloudataConf(job);
      CTable ctable = CTable.openTable(nconf, job.get(DefaultTabletInputFormat.OUTPUT_TABLE));
      String columns = job.get(DefaultTabletInputFormat.INPUT_COLUMN_LIST);
      uploader = ctable.openDirectUploader(columns.split(","));
      
    } catch (Exception e) {
      confErr = new IOException(e.getMessage());
      confErr.initCause(e);
    }
  }

  @Override
  public void close() throws IOException {
    if(uploader != null) {
      uploader.close();
    }
    LOG.info("total " + count + " cells restored");    
  }
}
