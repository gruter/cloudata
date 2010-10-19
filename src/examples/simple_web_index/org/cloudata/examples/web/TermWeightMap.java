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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

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


public class TermWeightMap implements Mapper<Row.Key, Row, WritableComparable, Writable> {
  static final Log LOG = LogFactory.getLog(TermWeightMap.class.getName());
  
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
    
    byte[] contentsBytes = contentsCells.get(0).getBytes();
    if(contentsBytes == null || contentsBytes.length == 0) {
      LOG.info("Contents is null: " + documentId);
      return;
    }
    
    String contents = new String(contentsBytes, 0, contentsBytes.length, "EUC-KR").trim();
    if(contents.length() == 0) {
      return;
    }
    
    Map<String, Integer> termFrequency = parseContents(contents);
    
    for(Map.Entry<String, Integer> entry: termFrequency.entrySet()) {
      String eachTerm = entry.getKey();
      eachTerm = eachTerm.trim();
      if(eachTerm.length() < TestWebPage.MIN_TERM_LENGTH) {
        continue;
      }
      int freq = entry.getValue();
      
      byte[] termBytes = eachTerm.getBytes("EUC-KR");
      count++;
      if(count % 10000 == 0) {
        System.out.println("Term[" + new String(termBytes, "EUC-KR") + "]");
      }
      collector.collect(new Text(termBytes), new Text(documentId + "\t" + freq + "\t" + contentsBytes.length));
    }
  }

  private Map<String, Integer> parseContents(String contents) throws IOException {
//    Map<String, Integer> termFrequency = new HashMap<String, Integer>();
//    try {
//      MorphemeAnalyzer analyzer = new MorphemeAnalyzer();
//      String[] words = contents.split("([-~?<>=”“△/(),.·;’‘\\s\\[\\]\\n\\r])+");
//      for( int i = 0 ; i < words.length; i++) {
//        HashSet parsedWords = analyzer.parse(words[i]);
//        for(Iterator it = parsedWords.iterator(); it.hasNext(); ) {
//          String term = (String)it.next();
//          if(termFrequency.containsKey(term)) {
//            termFrequency.put(term, termFrequency.get(term) + 1);
//          } else {
//            termFrequency.put(term, 1);
//          }
//        }
//      }
//    } catch(Exception e) {
//      throw new IOException(e.getMessage());
//    }
    
    Map<String, Integer> termFrequency = new HashMap<String, Integer>();
    
    //String[] terms = StringUtils.split(contents, " ");
    String[] terms = contents.split("([-~?<>=”“△/(),.·;’‘\\s\\[\\]\\n\\r])+");
    
    for(String term: terms) {
      term = term.trim();
      if(term.length() < 2) {
        //글자수가 2보다 작은 글자는 무시(너무 많아서)
        continue;
      }
      if(termFrequency.containsKey(term)) {
        termFrequency.put(term, termFrequency.get(term) + 1);
      } else {
        termFrequency.put(term, 1);
      }
    }
    return termFrequency;
  }

  public void configure(JobConf jobConf) {
  }

  public void close() throws IOException {
    LOG.info(count + " term parsed");
  }
  
  public static void main(String[] args) throws Exception {
    //MorphemeAnalyzer al = new MorphemeAnalyzer();
      String file = args[0];
          
      BufferedReader br = new BufferedReader(new FileReader(file));
      
      String line = null;
      HashSet result ;
      try {
        while(( line = br.readLine()) != null)
        {
          String[] words = line.split("([-~?<>=”“△/(),.·;’‘\\s\\[\\]\\n\\r])+");
          
          for( int i = 0 ; i < words.length; i++)
          {
            System.out.println(words[i]);
//            result = al.parse(words[i]);
//            System.out.println(words[i] + "=>"+ CharUtil.arrayToString(result));
          }
        }
              
//        System.out.println("----- UNREGISTERED ------");
//        HashMap unreg = al.getUnregistered();
//        Iterator iter = unreg.keySet().iterator();
//        while( iter.hasNext())
//        {
//          String k = (String)iter.next();
//          ArrayList v = (ArrayList) unreg.get(k);
//          //System.out.println(k + "~>" + CharUtil.arrayToString(v));
//        }

      } 
      finally
      {
        br.close();
      }    
  }
}
