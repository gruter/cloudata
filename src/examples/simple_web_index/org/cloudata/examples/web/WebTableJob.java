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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.tablet.TableSchema;


/**
 * 실제 데이터를 이용하여 계속해서 등록/조회/삭제 처리를 수행하는 테스트
 * Map을 이용하여 멀티 클라이언트 환경 구성, 계속 반복 수행
 * webtable은 온라인 테스트용, termtable은 배치 테스트용
 * webtable에는 모든 데이터 insert, termtable에 webtable로 부터 term을 추출하여 insert
 * @author babokim
 *
 */
public class WebTableJob {
  public static final Log LOG = LogFactory.getLog(WebTableJob.class.getName()); 
  public static final String WEB_TABLE = "T_WEB";
  public static final String[] WEB_TABLE_COLUMNS = {"url", "title", "contents", "anchor"};
  
  public void exec(String[] options) throws Exception {
    if(options.length < 1) {
      System.out.println("Usage: java TestWebPage <num of repeats> webtable <inputPath>");
      System.exit(0);
    }
    //WebTable 생성
    CloudataConf nconf = new CloudataConf();
    if(!CTable.existsTable(nconf, WEB_TABLE)) {
      TableSchema webTableInfo = new TableSchema(WEB_TABLE, "Test", WEB_TABLE_COLUMNS);
      webTableInfo.setNumOfVersion(2);
      CTable.createTable(nconf, webTableInfo);
    }
    
    Path tempOutputPath = new Path("WebTableJob_" + System.currentTimeMillis());
    
    JobConf jobConf = new JobConf(WebTableJob.class);
    jobConf.setJobName("WebTableJob" + "(" + new Date() + ")");
    FileInputFormat.addInputPath(jobConf, new Path(options[0]));
    
    //<MAP>
    jobConf.setMapperClass(WebTableMap.class);
    jobConf.setInputFormat(TextInputFormat.class);
    jobConf.setMaxMapAttempts(0);
    //</MAP>

    //Map Only
    jobConf.setNumReduceTasks(0);
    FileOutputFormat.setOutputPath(jobConf, tempOutputPath);
    
    //Run Job
    JobClient.runJob(jobConf);
    
    //delete temp output path
    FileSystem fs = FileSystem.get(jobConf);
    fs.delete(tempOutputPath, true);
  }

  static class WebTableMap implements Mapper<WritableComparable, Writable, Text, Text> {
    static final String DELIM = "==NEWLINE=="; 
    static final String TITLE = "title:"; 
    static final String LINK = "link:"; 
    static final String BODY = "body:"; 
    private List<String> docLines = new ArrayList<String>();
    private CTable webTable;
    int insertedCount = 0;
    private boolean skip = true;
    private static final int MAX_VALUE = 500 * 1024; //(500K)
    public void map(WritableComparable key, Writable value, 
        OutputCollector<Text, Text> collector, 
        Reporter reporter) throws IOException {
      Text tvalue = (Text)value;
      
      String valueStr = new String(tvalue.getBytes(), 0, tvalue.getLength(), "EUC-KR");
      
      if(DELIM.equals(valueStr)) {
        skip = false;
        if(docLines.size() > 0) {
          parseAndStoreDoc();
          docLines.clear();
        }
      } else {
        if(!skip) {
          docLines.add(valueStr);
        }
      }
    }

    private void parseAndStoreDoc() throws IOException {
      String link = null;
      String title = null;
      StringBuffer contents = new StringBuffer();
      
      for(String eachLine: docLines) {
        if(eachLine.startsWith(TITLE)) {
          title = getTitle(eachLine) + (new Date()).toString();
        } else if(eachLine.startsWith(LINK)) {
          link = getLink(eachLine);
        } else {
          if(contents.length() == 0 && eachLine.startsWith(BODY)) {
            contents.append(eachLine.substring(BODY.length()).trim()).append("\n");
          } else {
            contents.append(eachLine).append("\n");
          }
        }
      }
      
      contents.append((new Date()).toString());
      if(contents.length() > MAX_VALUE) {
        LOG.info("Skip " + link + " cause data too big: " + contents.length());
        return;
      }
      
      Row.Key rowKey = new Row.Key(String.valueOf(link.hashCode()));
      
      try {
        Row row = new Row(rowKey);
        row.addCell("url", new Cell(Cell.Key.EMPTY_KEY, link.getBytes()));
        row.addCell("title", new Cell(Cell.Key.EMPTY_KEY, link.getBytes()));
        row.addCell("contents", new Cell(Cell.Key.EMPTY_KEY, contents.toString().getBytes("EUC-KR")));
        webTable.put(row);
        insertedCount++;
        if(insertedCount % 100 == 0) {
          LOG.info(insertedCount + " inserted, rowKey=" + link + ", title=" + title);
        }
      } catch (Exception e) {
        LOG.error(rowKey + "," + e.getMessage(), e);
        return;
      } 
    }
    
    private String getTitle(String line) throws IOException {
      String title = line.substring(TITLE.length()).trim();
      return title;
    }
    
    private String getLink(String line) throws IOException {
      String link = line.substring(LINK.length()).trim();
      return link;
    }
    
    public void configure(JobConf jobConf) {
      try {
        CloudataConf conf = new CloudataConf();
        webTable = CTable.openTable(conf, WEB_TABLE);
      } catch (IOException e) {
        LOG.error(e);
      }
    }

    public void close() throws IOException {
      LOG.info("Total Insert Record: " + insertedCount);
    }
  }
}
