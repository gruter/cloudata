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
package org.cloudata.util.matrix;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.ScanCell;
import org.cloudata.core.client.scanner.ScannerFactory;
import org.cloudata.core.client.scanner.TableScanner;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.tablet.ColumnValue;


public class Matrix extends AbstractMatrix {
  private static final Log LOG = LogFactory.getLog(Matrix.class.getName());
  
  TableScanner scanner;
  
  public Matrix(CloudataConf conf, String tableName, String columnName) throws IOException {
    super(conf, tableName, columnName);
  }

  @Override
  public void addToUploader(String row, String col, double value) throws IOException {
    if(uploader == null) {
      throw new IOException("Not uploader opened");
    }
    if(row == null) {
      row = "";
    }
    Row.Key rowKey = new Row.Key(row + "," + col);
    Row tRow = new Row(rowKey);
    tRow.addCell(columnName, new Cell(new Cell.Key(col), 
        Double.toString(value).getBytes()));
    
    uploader.put(tRow);
  }

  @Override
  public void addValue(String row, String col, double value) throws IOException {
    if(row == null) {
      row = "";
    }
    Row.Key rowKey = new Row.Key(row + "," + col);

    Row tRow = new Row(rowKey);
    tRow.addCell(columnName, new Cell(new Cell.Key(col), Double.toString(value).getBytes()));
    ctable.put(tRow);
  }

  @Override
  protected void closeScanner() throws IOException {
    if(scanner != null) {
      scanner.close();
    }
  }

  @Override
  protected void initScanner() throws IOException {
    scanner = ScannerFactory.openScanner(ctable, columnName);
  }

  @Override
  protected void initScannerForTarget(String row) throws IOException {
    if(row == null) {
      row = "";
    }
    Row.Key startRowKey = new Row.Key(row + "," + "");
    Row.Key endRowKey = new Row.Key(row + "," + "~~~~~~~~~~~~~~~");
    
    scanner = ScannerFactory.openScanner(ctable, startRowKey, endRowKey, columnName);
  }

  @Override
  protected void closeScannerForTarget() throws IOException {
    if(scanner != null) {
      scanner.close();
    }
  }
  
  @Override
  protected MatrixItem nextItemForTarget() throws IOException {
    return nextItem();
  }
  
  @Override
  protected boolean isSparse() throws IOException {
    return false;
  }
  
  @Override
  protected MatrixItem nextItem() throws IOException {
    ScanCell scanCell = scanner.next();
    if(scanCell == null) {
      return null;
    }
    
    Row.Key rowKey = scanCell.getRowKey();
    Cell.Key cellKey = scanCell.getCellKey();
    double value = Double.parseDouble(scanCell.getCellValue().getValueAsString());
    
    MatrixItem item = new MatrixItem();
    item.value = value;
    
    int rowKeyLength = rowKey.getByteSize();
    int columnKeyLength = cellKey.getByteSize();
    
    String row = "";
    if(rowKeyLength > columnKeyLength + 1) {
      int rowLength = rowKeyLength - (columnKeyLength + 1);
      byte[] rowBytes = new byte[rowLength];
      System.arraycopy(rowKey.getBytes(), 0, rowBytes, 0, rowLength);
      row = new String(rowBytes);
    }
    
    item.row = row;
    item.column = cellKey.toString();
    return item;
  }
  
  protected void print() throws IOException {
    TableScanner scanner = ScannerFactory.openScanner(ctable, columnName);
    try {
      Row row = null;
      while( (row = scanner.nextRow()) != null) {
        Row.Key rowKey = row.getKey();
        String[] rowKeySplit = rowKey.toString().split(",");
        for(Cell eachCell: row.getCellList(columnName)) {
          
          double value = Double.parseDouble(eachCell.getValue().getValueAsString());
          
          System.out.printf("%s, %s: %5.5f \n", rowKeySplit[0], rowKeySplit[1], value);
        }
      }
    } finally {
      scanner.close();
    }
  }
  
  public static void main(String[] args) throws Exception {
//    if(args.length < 4) {
//      System.out.println("Usage:java Matrix <InputTableName> <# Input rows> <# Input Cols> <ResultTableName>");
//    }
//    int numOfRows = Integer.parseInt(args[1]);
//    int numOfCols = Integer.parseInt(args[2]);
    
    CloudataConf conf = new CloudataConf();

    boolean insert = !CTable.existsTable(conf, "M1");
    Matrix matrix1 = new Matrix(conf, "M1", "Col1");
    
    if(insert) {
      matrix1.addValue("a", "a", 1);
      matrix1.addValue("a", "c", 1);
      matrix1.addValue("b", "a", 1);
      matrix1.addValue("b", "b", 1);
      matrix1.addValue("b", "c", 1);
      matrix1.addValue("c", "b", 1);
    }
    
//    int numOfRows = 100;
//    int numOfCols = 20;
//    matrix1.initUploader();
//   DecimalFormat keyFormat = new DecimalFormat("00000000000");
//    if(insert) {
//      Random rand = new Random();
//      for(int i = 1; i < numOfRows; i++) {
//        for(int j = 0; j < numOfCols; j++) {
//          matrix1.addToUploader(keyFormat.format(i), keyFormat.format(rand.nextInt(numOfCols) + 1), 1.0);
//        }
//        if(i % 10 == 0) {
//          System.out.println(i + " inserted");
//        }
//      }
//    }
//    matrix1.closeUploader();
    
    Matrix matrix2 = new Matrix(conf, "M1", "Col1");
    
    long startTime = System.currentTimeMillis();
    Matrix resultMatrix = new Matrix(conf, "M_RESULT", "Col1");
    matrix1.mutiplyLocal(matrix2, resultMatrix);
    
    LOG.info("End:" + (System.currentTimeMillis() - startTime));
    resultMatrix.print();
  }   
}
