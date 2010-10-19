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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.cloudata.core.client.Row;


/**
 * cloudata에 저장된 데이터를 파일시스템으로 flat한 파일로 저장하는 MapReduce Job의 Mapper 클래스
 * 데이터는 다음과 같은 순서로 저장 CSV 형태로 저장: rowkey, columnName, columnkey, value, timestamp
 * @author jindolk
 *
 */
public class BackupBinaryMap implements Mapper<Row.Key, Row, BytesWritable, BytesWritable> {
  static final Log LOG = LogFactory.getLog(BackupBinaryMap.class.getName());
  
  static BytesWritable EMPTY = new BytesWritable();
  
  BytesWritable outputValue = new BytesWritable();
  ByteArrayOutputStream bout;
  DataOutputStream out;
  
  @Override
  public void map(Row.Key key, Row value, OutputCollector<BytesWritable, BytesWritable> output,
      Reporter reporter) throws IOException {
    bout.reset();
    
    String[] columnNames = value.getColumnNames();
    if(columnNames == null || columnNames.length == 0) {
      return;
    }
    
    value.write(out);
    out.flush();
    
    outputValue.set(bout.toByteArray(), 0, bout.size());
    output.collect(outputValue, EMPTY);
  }

  @Override
  public void configure(JobConf job) {
    bout = new ByteArrayOutputStream();
    out = new DataOutputStream(bout); 
  }

  @Override
  public void close() throws IOException {
    if(out != null) {
      out.close();
    }
  }
}
