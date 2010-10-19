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
import org.cloudata.core.client.CellFilter.CellPage;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.tablet.TableSchema;


/**
 * @author jindolk
 *
 */
public class CellFilterExample {
  static String tableName = "T_FILTER_TEST";
  static DecimalFormat df = new DecimalFormat("0000000000");
  static CloudataConf conf = new CloudataConf();
  
  public static void main(String[] args) throws IOException { 
    insertTestData();
    
    //open table
    CTable ctable = CTable.openTable(conf, tableName);

    //specified cell key
    RowFilter rowFilter = new RowFilter(new Row.Key(df.format(10)));
    rowFilter.addCellFilter(new CellFilter("Col1", new Cell.Key(df.format(5))));
    
    Row row = ctable.get(rowFilter);
    System.out.println("============== Row:10, Cell:5 ==================");
    System.out.println("Selected cell: " + row.getKey() + "," + row.getFirst("Col1").getKey());
    
    //between 
    rowFilter = new RowFilter(new Row.Key(df.format(10)));
    rowFilter.addCellFilter(new CellFilter("Col1", new Cell.Key(df.format(5)), new Cell.Key(df.format(10))));
    
    row = ctable.get(rowFilter);
    System.out.println("============== Row:10, Cell:5 ~ 10 ==================");
    for(Cell eachCell: row.getCellList("Col1")) {
      System.out.println("Selected cell: " + eachCell.getKey());
    }    
    
    //Paging
    int pageSize = 7;
    rowFilter = new RowFilter(new Row.Key(df.format(10)));
    CellFilter cellFilter = new CellFilter("Col1", 
        new Cell.Key(df.format(23)), 
            new Cell.Key(df.format(45)));
    
    cellFilter.setCellPage(new CellFilter.CellPage(pageSize));  //7 items per page
    rowFilter.addCellFilter(cellFilter);
    
    System.out.println("============== paging (Cell:23 ~ 45) ==================");
    int page = 1;
    while( (row = ctable.get(rowFilter)) != null ) {
      for(Cell eachCell: row.getCellList("Col1")) {
        System.out.println("Selected cell: " + eachCell.getKey() + "," + eachCell.getValueAsString());
      }   
      
      cellFilter.setCellPage(row.getNextCellPage("Col1", pageSize));
      System.out.println("======= End Of " + page + " page ========");
      
      page++;
    }

    //User defined CellValueMatcher
    rowFilter = new RowFilter(new Row.Key(df.format(10)));
    cellFilter = new CellFilter("Col1",
        new Cell.Key(df.format(23)), 
        new Cell.Key(df.format(45)));
    cellFilter.setCellValueMatcher(UserCellValueMatcher.class, UserCellValueMatcher.VERSION, new String[]{"test_data_2"});
    rowFilter.addCellFilter(cellFilter);
    
    row = ctable.get(rowFilter);
    System.out.println("============== test_data_2% ==================");
    for(Cell eachCell: row.getCellList("Col1")) {
      System.out.println("Selected cell: " + eachCell.getKey());
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
      
      for(int j = 0; j < 50; j++) {
        row.addCell("Col1", new Cell(new Cell.Key(df.format(j)), ("test_data_" + j).getBytes()));
      }
      ctable.put(row);
    }
  }
}
