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

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.List;

import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CellFilter;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.RowFilter;
import org.cloudata.core.client.scanner.ScannerFactory;
import org.cloudata.core.client.scanner.TableScanner;
import org.cloudata.core.common.CloudataClusterTestCase;
import org.cloudata.core.tablet.TableSchema;
import org.cloudata.core.tablet.TabletInfo;
import org.cloudata.core.tablet.TableSchema.ColumnInfo;

import junit.framework.Test;
import junit.framework.TestSuite;


/**
 * CTable의 기본 기능에 대한 테스트
 * 
 * @author babokim
 * 
 */
public class TestCTable extends CloudataClusterTestCase {
  public static boolean PRINT = false;
  String tableName = "T_TEST_TABLE01";
  
  public TestCTable(String name) {
    super(name);
  }

  public static Test suite() {
    TestSuite suite = new TestSuite();
    suite.addTest(new TestCTable("testTable"));
    suite.addTest(new TestCTable("testPutAndGet"));
    suite.addTest(new TestCTable("testBlob"));
    return suite;
  }
  
  public static void main(String[] args) throws Exception {
    junit.textui.TestRunner.run(TestCTable.suite());
  }
  
  public void setUp() throws Exception {
    super.setUp();

    CTable.dropTable(conf, tableName);
  }
  
  public void tearDown() throws Exception {
    super.tearDown();
  }
  
  /**
   * Test table creation, addColumn, drop table
   * @throws Exception
   */
  public void testTable() throws Exception {
    //normal creation
    TableSchema tableSchema = new TableSchema(tableName);
    tableSchema.addColumn(new ColumnInfo("Col1"));
    tableSchema.addColumn(new ColumnInfo("Col2"));
    tableSchema.addColumn(new ColumnInfo("Col3", TableSchema.BLOB_TYPE));
    tableSchema.setDescription("This is Test");
    tableSchema.setNumOfVersion(5);
    
    CTable.createTable(conf, tableSchema);
    
    assertTable(tableSchema);

    //add column
    CTable ctable = CTable.openTable(conf, tableName);
    assertNotNull(ctable);
    
    ctable.addColumn("Col4");
    
    TableSchema createdTableSchema = TableSchema.loadTableSchema(conf, zk, tableName);
    assertEquals("Col4", createdTableSchema.getColumnInfos().get(3).getColumnName());
    
    CTable.dropTable(conf, tableName);
    
    assertNull(TableSchema.loadTableSchema(conf, zk, tableName));
    
    //table creation with table partition info
    Row.Key[] rowKeys = new Row.Key[]{new Row.Key("1000"), new Row.Key("2000"), new Row.Key("3000"), new Row.Key("4000")};
    
    CTable.createTable(conf, tableSchema, rowKeys);
    
    assertTable(tableSchema);
    
    ctable = CTable.openTable(conf, tableName);
    assertNotNull(ctable);
    TabletInfo[] tabletInfos = ctable.listTabletInfos();
    assertEquals(rowKeys.length + 1, tabletInfos.length);
    ctable.addColumn("Col4");
    createdTableSchema = TableSchema.loadTableSchema(conf, zk, tableName);
    assertEquals("Col4", createdTableSchema.getColumnInfos().get(3).getColumnName());
  }
  
  private void assertTable(TableSchema tableSchema) throws Exception{
    assertTrue(CTable.existsTable(conf, tableName));
    TableSchema createdTableSchema = TableSchema.loadTableSchema(conf, zk, tableName);
    assertEquals(tableName, createdTableSchema.getTableName());
    assertEquals(3, createdTableSchema.getColumns().size());
    assertEquals("Col1", createdTableSchema.getColumnInfos().get(0).getColumnName());
    assertEquals("Col2", createdTableSchema.getColumnInfos().get(1).getColumnName());
    assertEquals("Col3", createdTableSchema.getColumnInfos().get(2).getColumnName());
    assertEquals(TableSchema.BLOB_TYPE, createdTableSchema.getColumnInfos().get(2).getColumnType());
    assertEquals("This is Test", createdTableSchema.getDescription());
    assertEquals(5, createdTableSchema.getNumOfVersion());
    assertEquals(System.getProperty("user.name"), createdTableSchema.getOwner());
  }
  
  /**
   * Test NTable's put, get, gets, iterator, scanner, hasValue and Tablet split
   * @throws Exception
   */
  public void testPutAndGet() throws Exception {
    createTestTable();
    
    CTable ctable = CTable.openTable(conf, tableName);

    //1. insert test data
    System.out.println("================================== insert test data ==================================");
    DecimalFormat df = new DecimalFormat("0000000000");
    int index = 0;
    while(true) {
      Row.Key rowKey = new Row.Key(df.format(index));
      
      Row row = new Row(rowKey);
      row.addCell("Col1", new Cell(Cell.Key.EMPTY_KEY, generateTestData(1000, df.format(index)).getBytes()));
      
      //insert different col key data in a row
      row.addCell("Col2", new Cell(new Cell.Key(df.format(index)), "d1".getBytes()));
      row.addCell("Col2", new Cell(new Cell.Key(df.format(index+1)), "d2".getBytes()));
      
      ctable.put(row);
      
      Row selectedRow = ctable.get(rowKey);
      assertTrue(row.deepEquals(selectedRow));
      
      //if table splited. stop put
      if(index > 0 && index % 1000 == 0) {
        TabletInfo[] tabletInfos = ctable.listTabletInfos();
        if(tabletInfos.length > 1) {
          break;
        }
        Thread.sleep(2 * 1000);
      }

      index++;
      if(index > 50000) {
        fail("Table not splited: " + index + " inserted");
      }
    }
    
    //2. assert inserted data
    System.out.println("================================== assert inserted data ==================================");
    PRINT = true;
    for(int i = 0; i < index; i++) {
      Row.Key rowKey = new Row.Key(df.format(i));
      Row selectedRow = ctable.get(rowKey);
      
      assertNotNull(selectedRow);
      
      List<Cell> cells = selectedRow.getCellList("Col1");
      assertNotNull(cells);
      assertEquals(1, cells.size());
      
      assertEquals(generateTestData(1000, df.format(i)), cells.get(0).getValueAsString());

      cells = selectedRow.getCellList("Col2");
      assertNotNull(rowKey + ", Col2 must not null", cells);
      assertEquals(2, cells.size());
      assertEquals(new Cell.Key(df.format(i)), cells.get(0).getKey());
      assertEquals("d1", cells.get(0).getValueAsString());
      assertEquals(new Cell.Key(df.format(i+1)), cells.get(1).getKey());
      assertEquals("d2", cells.get(1).getValueAsString());
    }
    
    //3. assertPagingGet
    System.out.println("================================== assertPagingGet ==================================");
    RowFilter rowFilter = new RowFilter(new Row.Key(df.format(index/10)), 
        new Row.Key(df.format(index/10 * 9)), RowFilter.OP_BETWEEN);
    rowFilter.addCellFilter(new CellFilter("Col2"));
    
    Row.Key previousPageEndRowKey = null;
    rowFilter.setPagingInfo(10, previousPageEndRowKey);
    
    int count = 0;
    while(true) {
      Row[] rows = ctable.gets(rowFilter);
      if(rows == null) {
        break;
      }
      assertTrue(rows.length > 0 && rows.length <= 10);
      
      previousPageEndRowKey = rows[rows.length -1].getKey();
      rowFilter.setPagingInfo(10, previousPageEndRowKey);
      
      for(int j = 0; j < rows.length; j++) {
        assertEquals(new Row.Key(df.format(index/10 + count)), rows[j].getKey());
        count++;
      }
    }
    assertEquals(index/10 * 9 - index/10, count - 1);
    
    //4. RowIterator
    System.out.println("================================== RowIterator ==================================");
    rowFilter = new RowFilter(new Row.Key(df.format(index/10)), 
        new Row.Key(df.format(index/10 * 9)), RowFilter.OP_BETWEEN);
    rowFilter.addCellFilter(new CellFilter("Col2"));
    Iterator<Row> rowIterator = ctable.iterator(rowFilter);
    count = 0;
    while(rowIterator.hasNext()) {
      Row eachRow = rowIterator.next();
      assertNotNull(eachRow);
      assertEquals(new Row.Key(df.format(index/10 + count)), eachRow.getKey());
      count++;
    }
    assertEquals(index/10 * 9 - index/10, count - 1);
    
    //5. assertScanner
    System.out.println("================================== assertScanner ==================================");
    rowFilter = new RowFilter(new Row.Key(df.format(index/10)), new Row.Key(df.format(index/10 * 9)));
    rowFilter.addCellFilter(new CellFilter("Col1"));
    rowFilter.addCellFilter(new CellFilter("Col2"));
    TableScanner scanner = ScannerFactory.openScanner(ctable, rowFilter);
    try {
      Row scannedRow = null;
      count = 0;
      while( (scannedRow = scanner.nextRow()) != null ) {
        assertEquals(new Row.Key(df.format(index/10 + count)), scannedRow.getKey());
        List<Cell> cells = scannedRow.getCellList("Col1");
        assertNotNull("RowKey:" + scannedRow.getKey().toString(), cells);
        assertEquals(1, cells.size());
        
        assertEquals(generateTestData(1000, df.format(index/10 + count)), cells.get(0).getValueAsString());
  
        cells = scannedRow.getCellList("Col2");
        assertNotNull("RowKey:" + scannedRow.getKey().toString(), cells);
        assertEquals(2, cells.size());
        assertEquals(new Cell.Key(df.format(index/10 + count)), cells.get(0).getKey());
        assertEquals("d1", cells.get(0).getValueAsString());
        assertEquals(new Cell.Key(df.format(index/10 + count + 1)), cells.get(1).getKey());
        assertEquals("d2", cells.get(1).getValueAsString());
        
        count++;
      }
    } finally {
      scanner.close();
    }
    assertEquals(index/10 * 9 - index/10, count - 1);
      
    //6. hasValue
    System.out.println("================================== hasValue ==================================");
    assertTrue(ctable.hasValue("Col1", new Row.Key(df.format(index/10))));
    assertFalse(ctable.hasValue("Col1", new Row.Key(df.format(index/10) + "1")));

    assertTrue(ctable.hasValue("Col2", new Row.Key(df.format(index/10)), new Cell.Key(df.format(index/10))));
    assertFalse(ctable.hasValue("Col2", new Row.Key(df.format(index/10)), new Cell.Key(df.format(index/10) + "1")));
    
    //drop table
    System.out.println("================================== dropTable ==================================");
    CTable.dropTable(conf, tableName);
    assertNull(TableSchema.loadTableSchema(conf, zk, tableName));
  }
  
  public void testBlob() throws Exception {
    createTestTable();
    CTable ctable = CTable.openTable(conf, tableName);
    
    Row.Key rowKey = new Row.Key("BLOB_TEST");
    OutputStream out = ctable.createBlob(rowKey, "Col3", Cell.Key.EMPTY_KEY);
  
    long writeLength = 0;
    DecimalFormat df = new DecimalFormat("0000000000");
    String data = "012345678900123456789001234567890012345678900123456789001234567890012345678900123456789001234567890";
    for(int i = 0; i < 100; i++) {
      byte[] writeData = (df.format(i) + data + "\n").getBytes();
      out.write(writeData);
      writeLength += writeData.length; 
    }
    out.close();
    
    InputStream in = ctable.openBlob(rowKey, "Col3", Cell.Key.EMPTY_KEY);
    byte[] buf = new byte[(int)writeLength/100];
    int index = 0;
    int readLength = 0;
    int readSum = 0;
    while((readLength = in.read(buf)) > 0) {
      readSum += readLength;
      String readData = new String(buf, 0, readLength);
      assertEquals(df.format(index) + data + "\n", readData);
      index++;
    }
    in.close();
    
    assertEquals(writeLength, readSum);
    assertEquals(100, index);
    
    //BufferedInputStream
    BufferedInputStream bin = new BufferedInputStream(ctable.openBlob(rowKey, "Col3", Cell.Key.EMPTY_KEY));
    buf = new byte[(int)writeLength/100];
    index = 0;
    readLength = 0;
    readSum = 0;
    while((readLength = bin.read(buf)) > 0) {
      readSum += readLength;
      String readData = new String(buf, 0, readLength);
      assertEquals(df.format(index) + data + "\n", readData);
      index++;
    }
    bin.close();
    
    assertEquals(writeLength, readSum);
    assertEquals(100, index);
    
    //open BufferedReader
    BufferedReader reader = new BufferedReader(new InputStreamReader(ctable.openBlob(rowKey, "Col3", Cell.Key.EMPTY_KEY)));
    String line = null;
    index = 0;
    while((line = reader.readLine()) != null) {
      assertEquals(df.format(index) + data, line);
      index++;
    }
    reader.close();
    
    assertEquals(100, index);
  }
  
  private void createTestTable() throws Exception {
    TableSchema tableSchema = new TableSchema(tableName);
    tableSchema.addColumn(new ColumnInfo("Col1"));
    tableSchema.addColumn(new ColumnInfo("Col2", TableSchema.CACHE_TYPE));
    tableSchema.addColumn(new ColumnInfo("Col3", TableSchema.BLOB_TYPE));
    tableSchema.setDescription("This is Test");
    tableSchema.setNumOfVersion(5);
    
    CTable.createTable(conf, tableSchema);    
  }
  
  private String generateTestData(int length, String testData) {
    StringBuilder sb = new StringBuilder(length);
    for(int i = 0; i < length; i++) {
      sb.append(testData);
    }
    
    return sb.toString();
  }
}  
