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
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.conf.CloudataConf;


public class DocFreqReduce implements
    Reducer<WritableComparable, Writable, WritableComparable, Writable> {
  static final Log LOG = LogFactory.getLog(DocFreqReduce.class.getName());

  private IOException exception;

  CTable termTable;

  public void reduce(WritableComparable key, Iterator<Writable> values,
      OutputCollector<WritableComparable, Writable> collector, Reporter reporter)
      throws IOException {
    if (exception != null) {
      throw exception;
    }
    Text tKey = (Text) key;
    Row.Key rowKey = new Row.Key(tKey.getBytes(), 0, tKey.getLength());

    int docFreq = 0;
    while (values.hasNext()) {
      docFreq++;
    }

    Row row = new Row(rowKey);
    try {
      row.addCell("df", new Cell(Cell.Key.EMPTY_KEY, Long.toString(docFreq).getBytes()));
      termTable.put(row);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public void configure(JobConf jobConf) {
    try {
      CloudataConf conf = new CloudataConf();

      termTable = CTable.openTable(conf, TermUploadJob.TERM_TABLE);
    } catch (IOException e) {
      exception = e;
    }
  }

  public void close() throws IOException {
  }
}
