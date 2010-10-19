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
import java.io.OutputStream;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.cloudata.core.tablet.ColumnValue;


public class TermWeightReduceOnline implements Reducer<WritableComparable, Writable, WritableComparable, Writable> {
  static final Log LOG = LogFactory.getLog(TermWeightReduce.class.getName());
//  private NTable termTable;
  private IOException exception;
  
  long sumDocLength;
  long sumDocCount;
  long avgDocLength;

  int termCount;
  OutputStream partitionOut;
  
  DecimalFormat df = new DecimalFormat("0.0000000000");
  
  CTable termTable;
  
  String[] insertColumns = new String[]{"tf", "weight", "i_weight"};
  
  public void reduce(WritableComparable key, 
      Iterator<Writable> values, 
      OutputCollector<WritableComparable, Writable> collector, 
      Reporter reporter) throws IOException {
    if(exception != null) {
      throw exception;
    }
    //key: term, value: documentId , freq, docLength
    Text tKey = (Text)key;
    Row.Key rowKey = new Row.Key(tKey.getBytes(), 0, tKey.getLength());
    String keyStr = new String(tKey.getBytes(), 0, tKey.getLength(), "EUC-KR");
    
    if(tKey.getLength() == 0 || keyStr.trim().length() < TestWebPage.MIN_TERM_LENGTH) {
      return;
    }
    
    Row row = termTable.get(rowKey, "df");
    if(row == null || row.getColumnSize() == 0) {
      LOG.error("No df for term:" + keyStr);
      return;
    }
    
    int docFreq = row.getOne("df").getValue().getValueAsInt();

    Row iRow = new Row(rowKey);
    int count = 0;
    List<ColumnValue> tfColumnValues = new ArrayList<ColumnValue>();
    List<ColumnValue> weightColumnValues = new ArrayList<ColumnValue>();
    List<ColumnValue> iWeightColumnValues = new ArrayList<ColumnValue>();
    
    while(values.hasNext()) {
      Text tValue = (Text)values.next();
      String valueStr = tValue.toString();
      String[] valueTokens = valueStr.split("\t");
      if(valueTokens.length < 3) {
        LOG.error("valueTokens != 3:" + valueStr);
        return;
      }
      String documentId = valueTokens[0];
      int freq = Integer.parseInt(valueTokens[1]);
      long docLength = Long.parseLong(valueTokens[2]);

      double weight = getTermWeight(freq, docLength, avgDocLength, sumDocCount, docFreq);

      iRow.addCell("tf", new Cell(new Cell.Key(documentId), Integer.toString(freq).getBytes()));
      iRow.addCell("weigth", new Cell(new Cell.Key(documentId), Integer.toString(freq).getBytes()));
      
      byte[] documentIdBytes = documentId.getBytes();
      iRow.addCell("i_weight", new Cell(new Cell.Key( (df.format(1.0-weight) + documentId).getBytes()), 
          documentIdBytes));
      
      if(termCount % 100000 == 0) {
        System.out.println("term=" + keyStr + ",document=" + documentId + ",freq=" + freq + ",df=" + docFreq + "weight=" + df.format(weight));
      }
      termCount++;
      
      count++;
      if(count % 500 == 0) {
        try {
          termTable.put(iRow);
        } catch (Exception e) {
          LOG.error(e);
        }
      }
      iRow = new Row(rowKey);
    }
    try {
      termTable.put(iRow);
    } catch (Exception e) {
      LOG.error(e);
    }
  }
  
  public void configure(JobConf jobConf) {
    try {
      CloudataConf conf = new CloudataConf();
      
      termTable = CTable.openTable(conf, TermUploadJob.TERM_TABLE);
      
      CTable globalTable = CTable.openTable(conf, TermGlobalJob.GLOBAL_TABLE);
      
      Row rows = globalTable.get(TermGlobalJob.GLOBAL_TABLE_ROWKEY);
      
      sumDocLength = rows.getOne("df").getValue().getValueAsLong();
      sumDocCount = rows.getOne("tf").getValue().getValueAsLong();
      
      if(sumDocLength == 0 || sumDocCount == 0) {
        exception = new IOException("doc length or # doc is zero");
      }
      avgDocLength = sumDocLength/sumDocCount;
      
      //key partition file
      FileSystem fs = FileSystem.get(jobConf);
      Path path = new Path("blogdata/tmp/weight/" + String.valueOf(System.currentTimeMillis()));
      fs.mkdirs(path.getParent());
      partitionOut = fs.create(path);
    } catch (IOException e) {
      exception = e;
    }
  }

  public void close() throws IOException {
    partitionOut.close();
  }
  
  static double getTermWeight(int tf, 
      long documentLength, 
      long avgDocLength, 
      long totalNumOfDoc, 
      int df) {
    double factorA = 0.75;
    double factorB = 2.0;
    double factorC = 0.1;
    double wtf = (((tf*(1-factorC))/(tf+((factorA+((1-factorA)*documentLength)/avgDocLength)*factorB))) + factorC);
    double widf = Math.log((totalNumOfDoc-df+0.5)/(df+0.5))/Math.log((totalNumOfDoc-1+0.5)/(1+0.5));
    return wtf*widf;
  }
          
  public static void main(String[] args) {
    long docLength = 3000;
    int tf = 161666;
    int nDoc = 2;  //1349
    long avgDocLength = 330443413/161666;
    int sumDocCount = 161666;
    
    System.out.println(getTermWeight(tf, docLength, avgDocLength, sumDocCount, nDoc));
  }
}
