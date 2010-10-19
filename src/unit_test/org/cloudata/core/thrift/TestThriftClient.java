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
package org.cloudata.core.thrift;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.tablet.TableSchema;
import org.cloudata.thrift.ThriftUtil;
import org.cloudata.thrift.CloudataThriftServer.CloudataHandler;
import org.cloudata.thrift.generated.ThriftCell;
import org.cloudata.thrift.generated.ThriftCellValue;
import org.cloudata.thrift.generated.ThriftColumnInfo;
import org.cloudata.thrift.generated.ThriftRow;
import org.cloudata.thrift.generated.ThriftTableSchema;


/**
 * @author jindolk
 *
 */
public class TestThriftClient extends TestCase {
  String tableName = "T_THRIFT_TEST";
  CloudataConf conf = new CloudataConf();
//  TTransport transport;
  CloudataHandler client ;
  
  public TestThriftClient(String name) throws Exception {
    super(name);
    client = new CloudataHandler();
  }
  
  public void setUp() throws Exception {
    if(CTable.existsTable(conf, tableName)) {
      CTable.dropTable(conf, tableName);
    }
  }
  
  public void tearDown() throws Exception {
    CTable.dropTable(conf, tableName);
  }
  
  public void testTable() throws Exception {
    //create table
    ThriftTableSchema testTableSchema = new ThriftTableSchema();
    testTableSchema.setTableName(tableName);
    testTableSchema.setNumOfVersion("2");
    testTableSchema.setDescription("thrift test");
    List<ThriftColumnInfo> columns = new ArrayList<ThriftColumnInfo>();
    columns.add(new ThriftColumnInfo("Col1", "2", "" + TableSchema.DEFAULT_TYPE));
    columns.add(new ThriftColumnInfo("Col2", "2", "" + TableSchema.DEFAULT_TYPE));
    testTableSchema.setColumns(columns);
    
    client.createTable(testTableSchema);
    
    assertTrue(CTable.existsTable(conf, tableName));
    
    //desc table
    ThriftTableSchema schema = client.descTable(tableName);
    assertNotNull(schema);
    assertEquals(tableName, schema.tableName);
    assertEquals("2", schema.numOfVersion);
    assertEquals(2, schema.getColumnsSize());
    assertEquals("Col1", schema.getColumns().get(0).columnName);
    assertEquals("Col2", schema.getColumns().get(1).columnName);
    
    //drop table
    client.dropTable("T_THRIFT_TEST");
    assertFalse(CTable.existsTable(conf, "T_THRIFT_TEST"));
  }
  
  public void testData() throws Exception {
    ThriftTableSchema testTableSchema = new ThriftTableSchema();
    testTableSchema.setTableName(tableName);
    testTableSchema.setDescription("thrift test");
    List<ThriftColumnInfo> columns = new ArrayList<ThriftColumnInfo>();
    columns.add(new ThriftColumnInfo("Col1", "2", "" + TableSchema.DEFAULT_TYPE));
    columns.add(new ThriftColumnInfo("Col2", "2", "" + TableSchema.DEFAULT_TYPE));
    testTableSchema.setColumns(columns);
    
    client.createTable(testTableSchema);
    
    //test put
    ThriftRow row1 = new ThriftRow();
    row1.setRowKey("RK1");
    Map<String, List<ThriftCell>> cells = new HashMap<String, List<ThriftCell>>();
    List<ThriftCell> col1Cells = new ArrayList<ThriftCell>();
    
    List<ThriftCellValue> cellValues = new ArrayList<ThriftCellValue>();
    cellValues.add(new ThriftCellValue("data1".getBytes(), "0", false));
    col1Cells.add(new ThriftCell("CK_11", cellValues));
    
    cellValues = new ArrayList<ThriftCellValue>();
    cellValues.add(new ThriftCellValue("data2".getBytes(), "0", false));
    col1Cells.add(new ThriftCell("CK_12", cellValues));
    cells.put("Col1", col1Cells);
    
    List<ThriftCell> col2Cells = new ArrayList<ThriftCell>();
    cellValues = new ArrayList<ThriftCellValue>();
    cellValues.add(new ThriftCellValue("data3".getBytes(), "0", false));
    col2Cells.add(new ThriftCell("CK_13", cellValues));
    cells.put("Col2", col2Cells);
    
    row1.setCells(cells);
    
    ThriftRow row2 = new ThriftRow();
    row2.setRowKey("RK2");
    Map<String, List<ThriftCell>> r2cells = new HashMap<String, List<ThriftCell>>();
    col1Cells = new ArrayList<ThriftCell>();
    cellValues = new ArrayList<ThriftCellValue>();
    cellValues.add(new ThriftCellValue("data2".getBytes(), "0", false));
    col1Cells.add(new ThriftCell("CK_21", cellValues));
    r2cells.put("Col1", col1Cells);
    row2.setCells(r2cells);
    
    client.put(tableName, row1, true);
    client.put(tableName, row2, true);
    
    CTable ctable = CTable.openTable(conf, tableName);
    assertTrue(ThriftUtil.getNRow(row1).deepEquals(ctable.get(new Row.Key("RK1"))));
    assertTrue(ThriftUtil.getNRow(row2).deepEquals(ctable.get(new Row.Key("RK2"))));
    
    //test get
    ThriftRow selectedRow = client.get(tableName, "RK1");
    assertNotNull(selectedRow);
    assertTrue(ThriftUtil.getNRow(row1).deepEquals(ThriftUtil.getNRow(selectedRow)));

    selectedRow = client.get(tableName, "RK2");
    assertNotNull(selectedRow);
    assertTrue(ThriftUtil.getNRow(row2).deepEquals(ThriftUtil.getNRow(selectedRow)));
    
    //test delete
    client.removeRow(tableName, "RK2");
    assertNull(ctable.get(new Row.Key("RK2")));
  }
  
  public static Test suite() throws Exception {
    TestSuite suite = new TestSuite();
    suite.addTest(new TestThriftClient("testTable"));
    suite.addTest(new TestThriftClient("testData"));
    return suite;
  }
  
  public static void main(String[] args) throws Exception {
    junit.textui.TestRunner.run(TestThriftClient.suite());
  }
}
