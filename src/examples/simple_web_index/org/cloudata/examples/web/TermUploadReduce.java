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

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Date;
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
import org.cloudata.core.client.DirectUploader;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.util.CloudataLineReader;
import org.cloudata.core.tablet.ColumnValue;


public class TermUploadReduce implements Reducer<WritableComparable, Writable, WritableComparable, Writable> {
  static final Log LOG = LogFactory.getLog(TermUploadReduce.class.getName());
  private CTable termTable;
  private IOException exception;
  private DirectUploader weightUploader;
  private DirectUploader dfUploader;
  
  int count = 0;
  DecimalFormat df = new DecimalFormat("0.0000000000");
  
  public void reduce(WritableComparable key, 
      Iterator<Writable> values, 
      OutputCollector<WritableComparable, Writable> collector, 
      Reporter reporter) throws IOException {
    if(exception != null) {
      throw exception;
    }
    Text tKey = (Text)key;
    int keyIndex = tKey.find("\t");
    if(keyIndex < 0) {
      LOG.error("invalid value:" + tKey);
      return;
    }
    
    Row.Key rowKey = new Row.Key(tKey.getBytes(), 0, keyIndex);
    
    String keyStr = new String(tKey.getBytes(), keyIndex + 1, (tKey.getLength() - keyIndex - 1), "EUC-KR");
    
    //term, 구분자(tf), documentId url, freq, weight
    //term, 구분자(df), df
    String[] valueTokens = keyStr.split("\t");
    
    if(rowKey.getLength() < TestWebPage.MIN_TERM_LENGTH) {
      return;
    }
    
    count++;
    if(count % 50000 == 0) {
      System.out.println(new Date() + ":" + keyStr);
    }
    
    if(valueTokens.length == 2 && "df".equals(valueTokens[0])) {
      Row row = new Row(rowKey);
      row.addCell("df", new Cell(Cell.Key.EMPTY_KEY, valueTokens[1].getBytes()));
      dfUploader.put(row);
    } else if(valueTokens.length == 4 && "tf".equals(valueTokens[0])) {
      Row row = new Row(rowKey);
      String documentId = valueTokens[1];
      String freq = valueTokens[2];
      String weight = valueTokens[3];
      
      row.addCell("tf", new Cell(new Cell.Key(documentId), freq.getBytes()));
      row.addCell("weight", new Cell(new Cell.Key(documentId), weight.getBytes()));
      
      byte[] documentIdBytes = documentId.getBytes();

      row.addCell("i_weight", new Cell(new Cell.Key( (df.format(1.0-Double.parseDouble(weight)) + documentId).getBytes()), 
          documentIdBytes));
      
      weightUploader.put(row);
    } else {
      LOG.error("invalid value:" + valueTokens.length + "," + count + "," + valueTokens[1] + "," + keyStr);
      return;
    }
  }

//  private byte[] getDescCell.Key(double weight, byte[] documentIdBytes) {
//    byte[] bytes = df.format(weight).getBytes();
//    
//    byte[] result = new byte[bytes.length + documentIdBytes.length];
//    for(int i = 0; i < bytes.length; i++) {
//      result[i] = (byte)(0xff - (int)bytes[i]);
//    }
//    
//    System.arraycopy(documentIdBytes, 0, result, bytes.length, documentIdBytes.length);
//    return result;
//  }
  
  public void configure(JobConf jobConf) {
    try {
      CloudataConf conf = new CloudataConf();
      
      termTable = CTable.openTable(conf, TermUploadJob.TERM_TABLE);
      
      weightUploader = termTable.openDirectUploader(new String[]{"tf", "weight", "i_weight"});
      dfUploader = termTable.openDirectUploader(new String[]{"df"});
    } catch (IOException e) {
      exception = e;
    }
  }

  public void close() throws IOException {
    LOG.info("Start closing");
    weightUploader.close();
    dfUploader.close();
    LOG.info("End closing");
  }
  

  public static void main(String[] args) throws IOException {
    CloudataConf nconf = new CloudataConf();
    TermUploadReduce reduce = new TermUploadReduce();
    reduce.configure(null);
    
    CloudataLineReader reader = new CloudataLineReader(
        new BufferedInputStream(new FileInputStream("D:/temp1/blog")));
    
    int count = 1;
    Text text = new Text();
    while(true) {
      int length = reader.readLine(text);
      if(length <= 0) {
        break;
      }
      reduce.reduce(text, null, null, null);
    }
    reader.close();
    reduce.close();
  }
}
