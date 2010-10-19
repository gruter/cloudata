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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

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
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.tablet.ColumnValue;


public class TermWeightReduce implements Reducer<WritableComparable, Writable, WritableComparable, Writable> {
  static final Log LOG = LogFactory.getLog(TermWeightReduce.class.getName());
//  private NTable termTable;
  private IOException exception;
  
  long sumDocLength;
  long sumDocCount;
  long avgDocLength;

  int termCount;
  OutputStream partitionOut;
  
  DecimalFormat df = new DecimalFormat("0.0000000000");
  
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
    
    List<Object[]> termFreqs = new ArrayList<Object[]>(100);
    Set<String> docs = new HashSet<String>();
    
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
      docs.add(documentId);
      
      termFreqs.add(new Object[]{documentId, freq, docLength});
      if(termFreqs.size() > 100000) {
        LOG.info("Too many tf:term=" + keyStr);
        break;
      }
    }
    int numOfdocument = docs.size();
    
    for(Object[] eachValue: termFreqs) {
      String documentId = (String)eachValue[0];
      int freq = (Integer)eachValue[1];
      long docLength = (Long)eachValue[2];
      
      double weight = getTermWeight(freq, docLength, avgDocLength, sumDocCount, numOfdocument);

      collector.collect(tKey, new Text("tf\t" + documentId + "\t" + String.valueOf(freq) + "\t" + df.format(weight)));
      termCount++;
      if(termCount % 100000 == 0) {
        System.out.println("term=" + keyStr + ",document=" + documentId + ",freq=" + freq + ",df=" + numOfdocument + "weight=" + df.format(weight));
      }
    }
    collector.collect(tKey, new Text("df\t" + numOfdocument));

    if(termCount % 100 == 0) {
      partitionOut.write(tKey.getBytes());
      partitionOut.write("\n".getBytes());
    }
  }
  
//  private BigDecimal getWeight(double docLength, double tf, double nDoc) {
//    //W = ( tf/(2.0 * ((1-0.75) + 0.75*(0.75*d-length/avg-d-length))) + tf) * log((N-n+0.5)/(n+0.5) )
//    BigDecimal nomalizedTf = new BigDecimal( tf/ ((2.0 * ((1.0-0.75) + 0.75*(0.75*docLength/avgDocLength))) + tf) );
//    BigDecimal nomalizedIdf = new BigDecimal(Math.log((sumDocCount-nDoc+0.5)/(nDoc+0.5))); 
//    return nomalizedTf.multiply(nomalizedIdf);
//  }
  
  public void configure(JobConf jobConf) {
    try {
      CloudataConf conf = new CloudataConf();
      
//      termTable = NTable.openTable(conf, TermWeightJob.TERM_TABLE);
      
      CTable globalTable = CTable.openTable(conf, TermGlobalJob.GLOBAL_TABLE);
      
      Row row = globalTable.get(TermGlobalJob.GLOBAL_TABLE_ROWKEY);
      
      
      sumDocLength = row.getOne("doclengh").getValue().getValueAsLong();
      sumDocCount =  row.getOne("doccount").getValue().getValueAsLong();
      
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
    if(partitionOut != null) {
      partitionOut.close();
    }
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
