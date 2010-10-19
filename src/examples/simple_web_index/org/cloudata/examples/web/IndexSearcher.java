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
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CellFilter;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.RowFilter;
import org.cloudata.core.client.CellFilter.CellPage;
import org.cloudata.core.common.conf.CloudataConf;


public class IndexSearcher {
  static DecimalFormat df = new DecimalFormat("000");
  static DecimalFormat keyDf = new DecimalFormat("0.0000000000");
  
  static final SortedMap<Cell.Key, Row> docResults = new TreeMap<Cell.Key, Row>();
  static CTable webTable;
  
  static long docInfoTime;
  public static void main(String[] args) throws Exception {
    
    CloudataConf conf = new CloudataConf();
    
    CTable termTable = CTable.openTable(conf, TermUploadJob.TERM_TABLE);
    webTable = CTable.openTable(conf, WebTableJob.WEB_TABLE);
    
    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    while(true) {
      System.out.print("Keyword(q:for exit): ");
      String line = reader.readLine();
      if("q".equals(line.toLowerCase())) {
        break;
      }
      
      String[] token = line.split(",");
      if(token.length == 2) {
        getUsingTermDocId(termTable, token[0], token[1]);
      } else {
        getUsingTerm(termTable, line);
      }
    }
    reader.close();
  }

  private static void getUsingTerm(CTable termTable, String term) throws Exception {
    long startTime = System.currentTimeMillis();
    Row.Key rowKey = new Row.Key(term.getBytes("EUC-KR"));
    
    RowFilter rowFilter = new RowFilter(rowKey);
    CellFilter columnFilter = new CellFilter("i_weight");
    //columnFilter.setNumOfValues(10);
    columnFilter.setCellPage(new CellPage(10, null, 0));
    rowFilter.addCellFilter(columnFilter);
    
    CellFilter dfColumnFilter = new CellFilter("df");
    rowFilter.addCellFilter(dfColumnFilter);
    
    Row row = termTable.get(rowFilter);

    List<Cell> weightCells = new ArrayList<Cell>(row.getCells("i_weight"));
    long docDf = row.getCells("df").get(0).getValue().getValueAsLong();
    
//    ColumnValue[] columnValues = termTable.get(rowKey, "i_weight");
    long iWeightGetTime = (System.currentTimeMillis() - startTime);
    
    if(weightCells == null || weightCells.size() == 0) {
      System.out.println("No data(time:" + (System.currentTimeMillis() - startTime) + ")");
      return;
    }
    
    int count = 0;
    
    long tfTimeSum = 0;
    long docInfoTimeSum = 0;
    System.out.println("---------------------------------------------");
    
    synchronized(docResults) {
      docResults.clear();
    }

    long docStartTime = System.currentTimeMillis();
    for(int i = 0; i < 10 && i < weightCells.size(); i++) {
      GetDoc getDoc = new GetDoc(weightCells.get(i).getKey(), weightCells.get(i).getValue().getBytes());
      getDoc.start();
//      ColumnValue[][] docInfos = webTable.get(new Row.Key(columnValues[i].getValue()), new String[]{"url","title"});
//      synchronized(docResults) {
//        docResults.put(columnValues[i].getCell.Key(), docInfos);
//        //docResults.notifyAll();
//      }
    }
    
    while(true) {
      synchronized(docResults) {
        docResults.wait();
        if(docResults.size() == weightCells.size()) {
          break;
        }
      }
    }
    
    docInfoTimeSum = System.currentTimeMillis() - docStartTime;
    int index = 1;
    for(Map.Entry<Cell.Key, Row> entry: docResults.entrySet()) {
      Row docInfos = entry.getValue();
      String iWeight = entry.getKey().toString();
      String weight = keyDf.format(1.0 - Double.parseDouble(iWeight.substring(0, 12)));
      
      System.out.println(
          //df.format(index) + "\t" +
          "docId=" + new String(docInfos.getKey().getBytes()) + "\t" + 
          "weight=" + weight + "\t" + 
          "url=" + docInfos.getCells("url").get(0).getPrintValue("EUC-KR") + "\t" + 
          "title=" + docInfos.getCells("title").get(0).getPrintValue("EUC-KR"));
      index++;
    }
    
//    for(ColumnValue eachColumnValue: columnValues) {
//      if(count >= 10) {
//        break;
//      }
//      byte[] docId = eachColumnValue.getValue();
//      
//      Cell.Key docCell.Key = new Cell.Key(docId);
//      
//      count++;
//      
//      String iWeight = eachColumnValue.getCell.Key().toString();
//      String weight = keyDf.format(1.0 - Double.parseDouble(iWeight.substring(0, 12)));
//      long docInfoTime = System.currentTimeMillis();
//      ColumnValue[][] docInfos = webTable.get(new Row.Key(docId), new String[]{"url","title"});
//      
//      docInfoTimeSum += System.currentTimeMillis() - docInfoTime;
//      
//      System.out.println(df.format(count) +
//          "\t docId=" + new String(docId) + 
////          "\t tf=" + new String(tf) + 
//          "\t weight=" + weight +
//          //"\t i_weight=" + eachColumnValue.getCell.Key().toString() + 
//          "\t url=" + docInfos[0][0].getPrintValue("EUC-KR") + 
//          "\t title=" + docInfos[1][0].getPrintValue("EUC-KR"));
//    }
    System.out.println(weightCells.size() + "/" + docDf + " values, total time:" + (System.currentTimeMillis() - startTime) + "," 
        + "query time=" + iWeightGetTime + ","
//        + "tf time=" + tfTimeSum + "," 
        + "docInfoTime=" + docInfoTimeSum + "," + docInfoTime);
        
    System.out.println("---------------------------------------------");
  }
  
  static class GetDoc extends Thread {
    byte[] docId;
    Cell.Key cellKey;
    
    public GetDoc(Cell.Key cellKey, byte[] docId) {
      this.docId = docId;
      this.cellKey = cellKey;
    }
    
    public void run() {
      try {
        long startTime = System.currentTimeMillis();
        Row docInfos = webTable.get(new Row.Key(docId), new String[]{"url","title"});
        long endTime = System.currentTimeMillis();
        synchronized(docResults) {
          docInfoTime += (endTime - startTime);
          docResults.put(cellKey, docInfos);
          docResults.notifyAll();
        }
      } catch (Exception e) { 
        e.printStackTrace();
        synchronized(docResults) {
          docResults.put(cellKey, null);
          docResults.notifyAll();
        }
      }
    }
  }
  
  private static void getUsingTermDocId(CTable termTable, String term, String docId) throws Exception {
    Row.Key rowKey = new Row.Key(term.getBytes("EUC-KR"));
    Cell.Key columnKey = new Cell.Key(docId);    
    
    byte[] weight = termTable.get(rowKey, "weight", columnKey);
    byte[] tf = termTable.get(rowKey, "tf", columnKey);
    
    Cell.Key iweightCellKey = new Cell.Key(keyDf.format(1.0 - Double.parseDouble(new String(weight))) + docId);
    byte[] i_weight = termTable.get(rowKey, "i_weight", iweightCellKey);
    
    System.out.println("tf:" + new String(tf) + ",weight:" + new String(weight) + ",i_weight:" + iweightCellKey + "," + new String(i_weight));
  }
}

