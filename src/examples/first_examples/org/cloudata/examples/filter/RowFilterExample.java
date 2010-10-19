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
package org.cloudata.examples.filter;

import java.io.IOException;
import java.text.DecimalFormat;

import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CellFilter;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.RowFilter;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.tablet.TableSchema;


/**
 * @author jindolk
 *
 */
public class RowFilterExample {
  static String tableName = "T_FILTER_TEST";
  static DecimalFormat df = new DecimalFormat("0000000000");
  static CloudataConf conf = new CloudataConf();
  
  public static void main(String[] args) throws IOException { 
    insertTestData();
    
    //open table
    CTable ctable = CTable.openTable(conf, tableName);
    
    //between
    RowFilter rowFilter = new RowFilter(
        new Row.Key(df.format(10)), 
        new Row.Key(df.format(15)),
        RowFilter.OP_BETWEEN);
    rowFilter.addCellFilter(new CellFilter("Col1"));
    
    Row[] rows = ctable.gets(rowFilter);
    System.out.println("============== selected from 10 to 15 ==================");
    for(Row eachRow: rows) {
      System.out.println("Selected row: " + eachRow.getKey());
    }
    
    //like
    rowFilter = new RowFilter(
        new Row.Key("TEST"),
        RowFilter.OP_LIKE);
    rowFilter.addCellFilter(new CellFilter("Col1"));
    
    rows = ctable.gets(rowFilter);
    System.out.println("============== selected like TEST% ==================");
    for(Row eachRow: rows) {
      System.out.println("Selected row: " + eachRow.getKey());
    }    
    
    //Paging
    rowFilter = new RowFilter(new Row.Key(df.format(10)), Row.Key.MAX_KEY, RowFilter.OP_BETWEEN);
    rowFilter.setPagingInfo(10, null);
    rowFilter.addCellFilter(new CellFilter("Col1"));
    
    System.out.println("============== paging ==================");
    int page = 1;
    while( (rows = ctable.gets(rowFilter)) != null ) {
      for(Row eachRow: rows) {
        System.out.println("Selected row: " + eachRow.getKey());
      }
      
      rowFilter.setPagingInfo(10, rows[rows.length - 1].getKey());
      System.out.println("======= End Of " + page + " page ========");
      
      page++;
    }
  }
  
  private static void insertTestData() throws IOException {
    if(CTable.existsTable(conf, tableName)) {
      CTable.dropTable(conf, tableName);
    }
    TableSchema tableSchema = new TableSchema(tableName);
    tableSchema.addColumn("Col1");
    
    CTable.createTable(conf, tableSchema);
    
    CTable ctable = CTable.openTable(conf, tableName);
    for(int i = 0; i < 50; i++) {
      Row row = new Row(new Row.Key(df.format(i + 1)));
      
      row.addCell("Col1", new Cell(Cell.Key.EMPTY_KEY, ("test_data_" + df.format(i + 1)).getBytes()));
      ctable.put(row);
    }
    
    for(int i = 50; i < 100; i++) {
      Row row = new Row(new Row.Key("TEST_" + df.format(i + 1)));
      
      row.addCell("Col1", new Cell(Cell.Key.EMPTY_KEY, ("test_data_" + df.format(i + 1)).getBytes()));
      ctable.put(row);
    }
  }
}
