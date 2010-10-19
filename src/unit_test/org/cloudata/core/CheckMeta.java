package org.cloudata.core;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.scanner.ScannerFactory;
import org.cloudata.core.client.scanner.TableScanner;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.tablet.TabletInfo;


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

/**
 * @author jindolk
 * 
 */
public class CheckMeta {
  public static void main(String[] args) throws Exception {
    CloudataConf conf = new CloudataConf();
    CTable ctable = CTable.openTable(conf, "META");
    
    TableScanner scanner = ScannerFactory.openScanner(ctable, "TabletInfo");
    
    Row scanRow = null;
    
    FileSystem fs = FileSystem.get(new Configuration());
    int count = 0;
    String previousRowKey = null;
    while ((scanRow = scanner.nextRow()) != null) {
      TabletInfo userTabletInfo = new TabletInfo();
      List<Cell> cells = scanRow.getCellList("TabletInfo");
      
      if(cells.size() > 1) {
        for(Cell eachCell: cells) {
          userTabletInfo.readFields(eachCell.getBytes());
          System.out.println(scanRow.getKey() + "," + eachCell.getKey() + "," + userTabletInfo);
        }
      }
    }
    scanner.close();
  }
}

