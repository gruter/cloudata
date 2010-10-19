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
package org.cloudata.core.client.blob;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.DecimalFormat;
import java.util.List;

import junit.framework.TestCase;

import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CellFilter;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.RowFilter;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.fs.CloudataFileSystem;
import org.cloudata.core.fs.GPath;
import org.cloudata.core.tablet.TableSchema;


/**
 * BLOB 처리에 대한 단순 기능 테스트
 * @author jindolk
 *
 */
public class TestBlob extends TestCase {
  private static final String blobTestTableName = "T_BLOB_TEST";
  private CloudataConf conf;
  private static final DecimalFormat df = new DecimalFormat("00000000000000000000"); //20 bytes
  private int NUM_OF_RECORD = 100;
  private BlobManager blobManager;
  
  public void setUp() throws Exception {
    conf = new CloudataConf();
  }
  
  public void tearDown() throws Exception {
    blobManager.shutdown();
  }
  
  public void testBlob() throws Exception {
    if(CTable.existsTable(conf, blobTestTableName)) {
      CTable.dropTable(conf, blobTestTableName);
    }
    
    CloudataFileSystem fs = CloudataFileSystem.get(conf);
    fs.delete(new GPath(conf.get("cloudata.root") + "/blobdata"), true);
    
    TableSchema tableSchema = new TableSchema(blobTestTableName);
    tableSchema.addColumn(new TableSchema.ColumnInfo("Column1"));
    tableSchema.addColumn(new TableSchema.ColumnInfo("Column2", TableSchema.BLOB_TYPE));
    
    CTable.createTable(conf, tableSchema);
    
    CTable ctable = CTable.openTable(conf, blobTestTableName);
    
    Row.Key rowKey = new Row.Key("RK1");
    Row row = new Row(rowKey);
    
    row.addCell("Column1", new Cell(new Cell.Key("CK1"), null));
    ctable.put(row);
    
    ///////////////////////////////////////////////////////////////////////////////
    int fileCount = 100;
    int writeLength = 0;
    for(int i = 0; i < fileCount; i++) {
      writeLength = createBlob(ctable, rowKey, "Column2", new Cell.Key(df.format(i + 1)));
      assertRead(ctable, rowKey, "Column2", new Cell.Key(df.format(i + 1)), writeLength);
    }
    System.out.println(fileCount + " files created. BlobManager started");
    
    blobManager = new BlobManager(conf);
    blobManager.start();
    
    while(getNumFiles() > 1) {
      Thread.sleep(1000);
    }
    
    byte[] fileInfo = ctable.get(rowKey, "Column2", new Cell.Key(df.format(2)));
    BlobColumnCell blobColumnCell = new BlobColumnCell(fileInfo);
    assertEquals("check file length after merging", writeLength, blobColumnCell.getLength());
    assertEquals("check file offset after merging", writeLength, blobColumnCell.getOffset());
    
    for(int i = 0; i < fileCount; i++) {
      assertRead(ctable, rowKey, "Column2", new Cell.Key(df.format(i + 1)), writeLength);
    }
    System.out.println("success asserting after merging1");
    ///////////////////////////////////////////////////////////////////////////////
    
    ///////////////////////////////////////////////////////////////////////////////
    //추가로 파일 더 저장
    rowKey = new Row.Key("RK2");
    row = new Row(rowKey);
    
    row.addCell("Column1", new Cell(new Cell.Key("CK1"), null));
    ctable.put(row);
    
    for(int i = fileCount; i < fileCount * 2; i++) {
      createBlob(ctable, rowKey, "Column2", new Cell.Key(df.format(i + 1)));
      assertRead(ctable, rowKey, "Column2", new Cell.Key(df.format(i + 1)), writeLength);
    }
    System.out.println(fileCount * 2 + " files created");
    
    //merge가 될때 까지 기다린다.
    while(getNumFiles() > 1) {
      Thread.sleep(1000);
    }
    for(int i = 0; i < fileCount; i++) {
      assertRead(ctable, new Row.Key("RK1"), "Column2", new Cell.Key(df.format(i + 1)), writeLength);
    }
    for(int i = fileCount; i < fileCount * 2; i++) {
      assertRead(ctable, rowKey, "Column2", new Cell.Key(df.format(i + 1)), writeLength);
    }
    System.out.println("success asserting after merging2");
    ///////////////////////////////////////////////////////////////////////////////

    blobManager.setMaxFileLength(fileCount * 2 * writeLength);

    ///////////////////////////////////////////////////////////////////////////////
    //remove
    for(int i = fileCount; i < fileCount * 2; i++) {
      ctable.remove(rowKey, "Column2", new Cell.Key(df.format(i + 1)));
    }
    System.out.println("blob column's data deleted");
    
    Thread.sleep(30 * 1000);
    
    for(int i = 0; i < fileCount; i++) {
      assertRead(ctable, new Row.Key("RK1"), "Column2", new Cell.Key(df.format(i + 1)), writeLength);
    }

    long tableFileLength = getFileLengthFromTable(ctable);
    long fsFileLength = getFileLengthFromFS();
    assertEquals(tableFileLength, fsFileLength);
    
    assertEquals(fileCount, getNumBlobMetaRecord());
    ///////////////////////////////////////////////////////////////////////////////
    
    CTable.dropTable(conf, blobTestTableName);
    
    assertFalse("blob table not exists after drop table.", CTable.existsTable(conf, BlobMetaManager.getBlobTableName(blobTestTableName, "Column2")));
    
    assertFalse("blob file root path not exists after drop table.", fs.exists(new GPath(BlobMetaManager.getBlobRootPath(conf, blobTestTableName, "Column2"))));
    assertFalse("blob file temp path not exists after drop table.", fs.exists(new GPath(BlobMetaManager.getBlobTempPath(conf, blobTestTableName, "Column2"))));
  }
  
  private int getNumBlobMetaRecord() throws IOException {
    CTable ctable = CTable.openTable(conf, BlobMetaManager.getBlobTableName(blobTestTableName, "Column2"));
    RowFilter rowFilter = new RowFilter(Row.Key.MIN_KEY, Row.Key.MAX_KEY, RowFilter.OP_BETWEEN);
    rowFilter.addCellFilter(new CellFilter(Constants.BLOB_COLUMN_FILE));
    rowFilter.addCellFilter(new CellFilter(Constants.BLOB_COLUMN_DELETE));
    
    Row[] rows = ctable.gets(rowFilter);
    
    int result = 0;
    for(Row eachRow: rows) {
      List<Cell> cells = eachRow.getCellList(Constants.BLOB_COLUMN_FILE);
      if(cells == null) {
        continue;
      }
      
      result += cells.size();
    }
    
    return result;
  }
  
  private long getNumFiles() throws IOException {
    GPath path = new GPath(BlobMetaManager.getBlobRootPath(conf, blobTestTableName, "Column2"));   
    
    CloudataFileSystem fs = CloudataFileSystem.get(conf);
    
    GPath[] paths = fs.list(path);
    
    return paths.length;
  }
  
  private long getFileLengthFromFS() throws IOException {
    long result = 0;
    GPath path = new GPath(BlobMetaManager.getBlobRootPath(conf, blobTestTableName, "Column2"));   
    
    CloudataFileSystem fs = CloudataFileSystem.get(conf);
    
    GPath[] paths = fs.list(path);
    
    for(GPath eachPath: paths) {
      result += fs.getLength(eachPath);
    }
    
    return result;
  }
  
  private long getFileLengthFromTable(CTable ctable) throws IOException {
    RowFilter rowFilter = new RowFilter(Row.Key.MIN_KEY, Row.Key.MAX_KEY, RowFilter.OP_BETWEEN);
    rowFilter.addCellFilter(new CellFilter("Column2"));
    
    Row[] rows = ctable.gets(rowFilter);
    
    long result = 0;
    for(Row eachRow: rows) {
      List<Cell> cells = eachRow.getCellList("Column2");
      if(cells == null) {
        continue;
      }
      
      for(Cell eachCell: cells) {
        BlobColumnCell blobColumnCell = new BlobColumnCell(eachCell.getBytes());
        result += blobColumnCell.getLength();
      }
    }
    
    return result;
  }
  
  private int createBlob(CTable ctable, Row.Key rowKey, String columnName, Cell.Key cellKey) throws IOException {
    Row row = new Row(rowKey);
    OutputStream out = ctable.createBlob(row.getKey(), columnName, cellKey);
    
    int writeLength = 0;
    
    try {
      for(int i = 0; i < NUM_OF_RECORD; i++) {
        byte[] data = (cellKey + df.format(i)).getBytes(); 
        out.write(data);
        
        writeLength += data.length;
      }
    } finally {
      out.close();
    }   
    
    return writeLength;
  }
  
  private void assertRead(CTable ctable, Row.Key rowKey, String columnName, Cell.Key cellKey, int writeLength) throws IOException {
    InputStream in = ctable.openBlob(rowKey, columnName, cellKey);
    try {
      
      int dataLength = (cellKey + df.format(0)).getBytes().length; 
      
      byte[] buf = new byte[dataLength];
      
      int totalReadLength = 0;
      int readLength = 0;
      int index = 0;
      
      while((readLength = in.read(buf)) > 0) {
        totalReadLength += readLength;
  
        String record = new String(buf, 0, readLength);
        
        assertEquals(cellKey + df.format(index), record);
        
        index++;
      }
      
      assertEquals(writeLength, totalReadLength);
    } finally {
      in.close();
    }
  }
  
  public static void main(String[] args) throws Exception {
    junit.textui.TestRunner.run(TestBlob.class);
  }
}
