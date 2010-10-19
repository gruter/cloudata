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
package org.cloudata.examples.first;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudata.core.client.AsyncDataReceiver;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CellFilter;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.tablet.TableSchema;


public class ParallelGetExam {
  public static void main(String[] args) throws IOException {
    //테이블 생성 및 데이터 입력
    String tableName = "SimpleTable2";
    String[] columns = new String[]{"Column1", "Column2"};
    String description = "테스트";
    TableSchema tableSchema = new TableSchema(tableName, description, columns);
    
    CloudataConf conf = new CloudataConf();
    if(!CTable.existsTable(conf, tableName)) {
      CTable.createTable(conf, tableSchema);
    }
    
    CTable ctable = CTable.openTable(conf, tableName);
    
    List<Row.Key> rowKeys = new ArrayList<Row.Key>();
    for(int i = 0; i < 100; i++) {
      Row.Key rowKey = new Row.Key(String.valueOf(i));
      Row row = new Row(rowKey);
      
      row.addCell("Column1", new Cell(Cell.Key.EMPTY_KEY, String.valueOf(i).getBytes()));
      ctable.put(row);
      
      rowKeys.add(rowKey);
    }
    
    //데이터 조회
    List<CellFilter> cellFilters = new ArrayList<CellFilter>();
    cellFilters.add(new CellFilter("Column1"));
    MyDataReceiver dataReceiver = new MyDataReceiver();
    int numOfThread = 10;
    int timeout = 50; //ms
    ctable.get(rowKeys, cellFilters, dataReceiver, numOfThread, timeout);
    
    for(Map.Entry<Row.Key, Row> entry: dataReceiver.results.entrySet()) {
      System.out.println(entry.getKey() + ": " + entry.getValue());
    }
  }
  
  static class MyDataReceiver implements AsyncDataReceiver {
    Map<Row.Key, Row> results = new HashMap<Row.Key, Row>();
    @Override
    public void error(Row.Key rowKey, Exception exception) {
      System.out.println("Error:" + rowKey + "," + exception);
    }

    @Override
    public void receive(Row.Key rowKey, Row row) throws IOException {
      results.put(rowKey, row);
    }

    @Override
    public void timeout() {
      System.out.println("timeout");
    }
  }
}
