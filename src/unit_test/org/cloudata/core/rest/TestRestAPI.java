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
package org.cloudata.core.rest;

import java.util.List;

import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.rest.CRestTable;
import org.cloudata.core.rest.CloudataRestService;
import org.cloudata.core.rest.RestWebServer;
import org.cloudata.core.tablet.TableSchema;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;


/**
 * @author jindolk
 *
 */
public class TestRestAPI extends TestCase {
  private RestWebServer restWebServer;
  private String tableName = "T_REST_TEST";
  private CloudataConf conf = new CloudataConf();
  
  public TestRestAPI(String name) {
    super(name);
    restWebServer = new RestWebServer();
    try {
      restWebServer.startWebServer(0);
    } catch (Exception e) {
      fail(e.getMessage());
    }
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
    CRestTable restTable = new CRestTable(conf);
    
    String description = "Test";
    
    String result = restTable.createTable(tableName, description, new String[]{"Col1", "Col2"});
    
    assertTrue(result.indexOf("Success") >= 0);
    
    CTable ctable = CTable.openTable(conf, tableName);
    
    assertEquals(tableName, ctable.descTable().getTableName());
    assertEquals(description, ctable.descTable().getDescription());
    assertEquals(2, ctable.descTable().getColumnInfos().size());
    assertEquals("Col1", ctable.descTable().getColumn(0));
    assertEquals("Col2", ctable.descTable().getColumn(1));

    
    String tableXml = restTable.descTable(tableName);
    TableSchema schema = CloudataRestService.getTableSchemaFromXml(tableXml);
    
    assertEquals(tableName, schema.getTableName());
    assertEquals(description, schema.getDescription());
    assertEquals(2, schema.getColumnInfos().size());
    assertEquals("Col1", schema.getColumn(0));
    assertEquals("Col2", schema.getColumn(1));
    
    result = restTable.dropTable(tableName);
    assertTrue(result.indexOf("Success") >= 0);
    
    assertFalse(CTable.existsTable(conf, tableName));
  }
  
  public void testData() throws Exception {
    CRestTable restTable = new CRestTable(conf);
    
    restTable.createTable(tableName, "Test", new String[]{"Col1", "Col2"});
    
    //test put
    Row row1 = new Row(new Row.Key("RK1"));
    row1.addCell("Col1", new Cell(new Cell.Key("CK_11"), "data1".getBytes()));
    row1.addCell("Col1", new Cell(new Cell.Key("CK_12"), "data2".getBytes()));
    row1.addCell("Col2", new Cell(new Cell.Key("CK_13"), "data3".getBytes()));
    
    Row row2 = new Row(new Row.Key("RK2"));
    row2.addCell("Col1", new Cell(new Cell.Key("CK_21"), "data4".getBytes()));
  
    String result = restTable.putRow(tableName, row1);
    assertTrue(result.indexOf("Success") >= 0);
    result = restTable.putRow(tableName, row2);
    assertTrue(result.indexOf("Success") >= 0);
    
    //test get
    CTable ctable = CTable.openTable(conf, tableName);
    assertTrue(row1.deepEquals(ctable.get(new Row.Key("RK1"))));
    assertTrue(row2.deepEquals(ctable.get(new Row.Key("RK2"))));
    
    List<Row> rows = CloudataRestService.getRowsFromXml(restTable.get(tableName, "RK1"));
    assertEquals(1, rows.size());
    assertTrue(row1.deepEquals(rows.get(0)));

    rows = CloudataRestService.getRowsFromXml(restTable.get(tableName, "RK2"));
    assertEquals(1, rows.size());
    assertTrue(row2.deepEquals(rows.get(0)));
    
    //test delete
    result = restTable.delete(tableName, "RK2");
    
    assertTrue(result.indexOf("Success") >= 0);
    assertNull(ctable.get(new Row.Key("RK2")));
  }
  
  public static Test suite() {
    TestSuite suite = new TestSuite();
    suite.addTest(new TestRestAPI("testTable"));
    suite.addTest(new TestRestAPI("testData"));
    return suite;
  }
  
  public static void main(String[] args) throws Exception {
    junit.textui.TestRunner.run(TestRestAPI.suite());
    System.exit(0);
  }
}
