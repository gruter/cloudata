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
package org.cloudata.core.client;

import java.io.IOException;
import java.text.DecimalFormat;

import org.cloudata.core.client.Cell;
import org.cloudata.core.client.DirectUploader;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.CloudataClusterTestCase;
import org.cloudata.core.tablet.TableSchema;

import junit.framework.Test;
import junit.framework.TestSuite;


/**
 * @author jindolk
 *
 */
public class TestDirectUploader extends CloudataClusterTestCase {
  String tableName = "T_TEST_UPLOADER";
  
  public TestDirectUploader(String name) {
    super(name);
  }
  
  public static Test suite() {
    TestSuite suite = new TestSuite();
    suite.addTest(new TestDirectUploader("testDefaultDirectUploader"));
    return suite;
  }
  
  public static void main(String[] args) throws Exception {
    junit.textui.TestRunner.run(TestDirectUploader.suite());
  }
  
  public void setUp() throws Exception {
    super.setUp();

    CTable.dropTable(conf, tableName);
  }
  
  public void tearDown() throws Exception {
    super.tearDown();
  }
  
  public void testDefaultDirectUploader() throws IOException {
    upload(true);
  }
  
  public void testSortDirectUploader() throws IOException {
    upload(false);
  }

  
  private void upload(boolean isDataSorted) throws IOException {
    TableSchema tableSchema = new TableSchema(tableName);
    tableSchema.addColumn("Col1");
    tableSchema.addColumn("Col2");
    
    Row.Key[] rowKeys = new Row.Key[5];
    for(int i = 0; i < 4; i++) {
      rowKeys[i] = new Row.Key(String.valueOf(i + 1));
    }
    rowKeys[4] = Row.Key.MAX_KEY;
    
    CTable.createTable(conf, tableSchema, rowKeys);
    
    CTable ctable = CTable.openTable(conf, tableName);
    
    DirectUploader uploader = ctable.openDirectUploader(tableSchema.getColumnsArray(), isDataSorted);
    
    DecimalFormat df = new DecimalFormat("0000000000");
    try {
      for(int i = 0; i < 5; i++) {
        for(int j = 0; j < 10; j++) {
          Row.Key rowKey = new Row.Key(i + "_" + df.format(j));
          
          Row row = new Row(rowKey);
          row.addCell("Col1", new Cell(Cell.Key.EMPTY_KEY, ("test01_" + df.format(j)).getBytes()));
          row.addCell("Col2", new Cell(new Cell.Key("CK_" + df.format(j)), ("test02_" + df.format(j)).getBytes()));
          
          uploader.put(row);
        }
      }
      uploader.close();
    } catch (IOException e) {
      uploader.rollback();
      throw e;
    } 
    
    for(int i = 0; i < 5; i++) {
      for(int j = 0; j < 10; j++) {
        Row.Key rowKey = new Row.Key(i + "_" + df.format(j));
        
        Row row = ctable.get(rowKey);
        assertNotNull(row);
        Cell cell = row.getFirst("Col1");
        assertNotNull(cell);
        assertEquals("test01_" + df.format(j), cell.getValueAsString());
        
        cell = row.getFirst("Col2");
        assertNotNull(cell);
        assertEquals("test02_" + df.format(j), cell.getValueAsString());
      }
    }
  }
}
