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
package org.cloudata.examples.upload;

import java.io.IOException;
import java.text.DecimalFormat;

import org.cloudata.core.client.Cell;
import org.cloudata.core.client.DirectUploader;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.tablet.ColumnValue;
import org.cloudata.core.tablet.TableSchema;


public class SimpleUploader {
  public static void main(String[] args) throws IOException {
    //테스트용 테이블 생성
    if(args.length < 1) {
      System.out.println("Usage:java SimpleUploader <table name>");
      System.exit(0);
    }
    TableSchema tableInfo = new TableSchema(args[0]);
    tableInfo.addColumn("title");
    tableInfo.addColumn("contents");
    
    CloudataConf conf = new CloudataConf();
    CTable.createTable(conf, tableInfo);
    
    //CTable 오픈
    CTable ctable = CTable.openTable(conf, tableInfo.getTableName());
    
    DirectUploader uploader = null;
    try {
      //Uploader 오픈
      uploader = ctable.openDirectUploader(tableInfo.getColumnsArray(), true);
    
    //테스트 데이터 업로드
      DecimalFormat df = new DecimalFormat("0000000000"); 
      for(int i = 0 ; i < 1000; i++) {
        Row.Key rowKey = new Row.Key(df.format(i));
        Row row = new Row(rowKey);
        //title
        row.addCell("title", new Cell(new Cell.Key("title" + i), null));
        //contents
        row.addCell("contents", new Cell(Cell.Key.EMPTY_KEY, ("contents" + df.format(i)).getBytes()));
        
        //업로드
        uploader.put(row);
      }
      //반드시 종료 처리해야 함
      uploader.close();
    } catch (Exception e) {
      //장애 발생시 rollback
      if(uploader != null) {
        uploader.rollback();
      }
    }
  }
}
