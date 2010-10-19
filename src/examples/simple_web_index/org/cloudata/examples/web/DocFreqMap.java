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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.Row;
import org.cloudata.core.tablet.ColumnValue;


public class DocFreqMap implements Mapper<Row.Key, Row, WritableComparable, Writable> {
  static final Log LOG = LogFactory.getLog(DocFreqMap.class.getName());
  
  int count = 0;
  public void map(Row.Key key, Row value, 
      OutputCollector<WritableComparable, Writable> collector, 
      Reporter reporter) throws IOException {
    String documentId = new String(key.getBytes(), 0, key.getLength(), "EUC-KR");
    
    //0: title, 1: contents
    
    if(value.getColumnSize() < 2) {
      LOG.warn("Length of RowColumnValuesArray less than 2: " + documentId);
      return;
    }
    
    List<Cell> titleCells = value.getCells(WebTableJob.WEB_TABLE_COLUMNS[1]);
    List<Cell> contentsCells = value.getCells(WebTableJob.WEB_TABLE_COLUMNS[2]);
    if(contentsCells == null) {
      LOG.warn("Contents column is empty : " + documentId);
      return;
    }
    
    //contents에 대해서 term으로 나눈 다음 term, document, freq를 write한다.
    byte[] contentsBytes = contentsCells.get(0).getBytes();
    if(contentsBytes == null || contentsBytes.length == 0) {
      LOG.info("Contents is null: " + documentId);
      return;
    }
    String contents = new String(contentsBytes, 0, contentsBytes.length, "EUC-KR").trim();
    if(contents.length() == 0) {
      return;
    }
    
    Set<String> uniqueTerms = parseContents(contents);
    
    for(String eachTerm: uniqueTerms) {
      eachTerm = eachTerm.trim();
      if(eachTerm.length() < TestWebPage.MIN_TERM_LENGTH) {
        continue;
      }
      byte[] termBytes = eachTerm.getBytes("EUC-KR");
      collector.collect(new Text(termBytes), new Text(documentId));
    }
    count++;
  }

  private Set<String> parseContents(String contents) throws IOException {
    Set<String> uniqueTerms = new HashSet<String>();
    
    //String[] terms = StringUtils.split(contents, " ");
    String[] terms = contents.split("([-~?<>=”“△/(),.·;’‘\\s\\[\\]\\n\\r])+");
    
    for(String term: terms) {
      term = term.trim();
      if(term.length() < TestWebPage.MIN_TERM_LENGTH) {
        //글자수가 2보다 작은 글자는 무시(너무 많아서)
        continue;
      }
      uniqueTerms.add(term);
    }
    return uniqueTerms;
  }

  public void configure(JobConf jobConf) {
  }

  public void close() throws IOException {
    LOG.info(count + " term parsed");
  }
}
