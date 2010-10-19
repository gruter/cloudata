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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class TermUploadMap implements Mapper<WritableComparable, Writable, WritableComparable, Writable> {
  static final Log LOG = LogFactory.getLog(TermUploadMap.class.getName());
  int count = 0;
  public void map(WritableComparable key, Writable value, 
      OutputCollector<WritableComparable, Writable> collector, 
      Reporter reporter) throws IOException {
    
    collector.collect((Text)value, new Text(""));
    count++;
    if(count % 50000 == 0) {
      Text tValue = (Text)value;
      String keyStr = new String(tValue.getBytes(), 0, tValue.getLength(), "EUC-KR");
      System.out.println(keyStr);
    }
  }

  public void configure(JobConf jobConf) {
  }

  public void close() throws IOException {
  }
}
