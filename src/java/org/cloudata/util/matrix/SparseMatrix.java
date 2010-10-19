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
import java.text.DecimalFormat;
import java.util.Random;

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
import org.cloudata.core.tablet.TabletInfo;


//bin/cloudata org.cloudata.util.matrix.SparseMatrix cal Y M_TEMP_06 M_DATA_1000000 M_DATA_1000000_385960278978892
//bin/cloudata org.cloudata.util.matrix.SparseMatrix cal Y M_TEMP_06 M_DATA_1000000 M_DATA_1000000_385862656886854

//bin/cloudata org.cloudata.util.matrix.SparseMatrix cal N M_TEMP_11 M_DATA_1000000

public class SparseMatrix extends AbstractMatrix {
  private static final Log LOG = LogFactory.getLog(Matrix.class.getName());

  TableScanner scanner;

  int scanIndex;

  Row rowData;

  String tabletName;
  
  long getTimeSum;
  
  int count = 0;
  public SparseMatrix(CloudataConf conf, String tableName,
      String columnName) throws IOException {
    super(conf, tableName, columnName);
  }
  public SparseMatrix(CloudataConf conf, String tableName,
      String columnName, AbstractMatrix inputMatrix) throws IOException {
    super(conf, tableName, columnName, inputMatrix);
  }

  public void setTabletName(String tabletName) {
    this.tabletName = tabletName;
  }
  @Override
  public void addToUploader(String row, String col, double value)
      throws IOException {
    if (uploader == null) {
      throw new IOException("Not uploader opened");
    }
    Row.Key rowKey = new Row.Key(row);
    Row tRow = new Row(rowKey);
    tRow.addCell(columnName, new Cell(new Cell.Key(col), Double.toString(value).getBytes()));

    uploader.put(tRow);
  }

  @Override
  public void addValue(String row, String col, double value) throws IOException {
    if (row == null) {
      row = "";
    }
    Row.Key rowKey = new Row.Key(row);

    Row tRow = new Row(rowKey);
    tRow.addCell(columnName, new Cell(new Cell.Key(col), Double.toString(value).getBytes()));
    ctable.put(tRow);

  }

  @Override
  protected void closeScanner() throws IOException {
    if (scanner != null) {
      scanner.close();
    }
  }

  @Override
  protected boolean isSparse() throws IOException {
    return true;
  }

  @Override
  protected void initScanner() throws IOException {
    if(tabletName != null) {
      TabletInfo tabletInfo = ctable.getTabletInfo(tabletName);
      if(tabletInfo == null) {
        throw new IOException("No tablet info:" + tabletName);
      }
      scanner = ScannerFactory.openScanner(ctable.getConf(), tabletInfo, columnName);
    } else {
      scanner = ScannerFactory.openScanner(ctable, columnName);
    }
  }

  @Override
  protected void initScannerForTarget(String row) throws IOException {
    long startTime = System.currentTimeMillis();
    rowData = ctable.get(new Row.Key(row), columnName);
    getTimeSum += (System.currentTimeMillis() - startTime);
    count++;
    if(count % 100 == 0) {
      System.out.println("GetTime:" + getTimeSum);
      getTimeSum = 0;
    }
    scanIndex = 0;
  }

  @Override
  protected void closeScannerForTarget() throws IOException {
    rowData = null;
    scanIndex = 0;
  }

  @Override
  protected MatrixItem nextItemForTarget() throws IOException {
    if (rowData == null || scanIndex >= rowData.getCellList(columnName).size()) {
      return null;
    }

    Cell cell = rowData.getCellList(columnName).get(scanIndex);
    MatrixItem item = convert(cell.getKey(), cell.getValue());
    scanIndex++;

    return item;
  }

  @Override
  protected MatrixItem nextItem() throws IOException {
    ScanCell scanCell = scanner.next();
    if (scanCell == null) {
      return null;
    }

    return convert(scanCell.getCellKey(), scanCell.getCellValue());
  }

  private MatrixItem convert(Cell.Key cellKey, Cell.Value cellValue) {
    Row.Key rowKey = rowData.getKey();
    double value = Double.parseDouble(cellValue.getValueAsString());

    MatrixItem item = new MatrixItem();
    item.row = rowKey.toString();
    item.column = cellKey.toString();
    item.value = value;
    return item;
  }

  protected void print() throws IOException {
    TableScanner scanner = ScannerFactory.openScanner(ctable, columnName);
    try {
      Row row = null;
      while ((row = scanner.nextRow()) != null) {
        Row.Key rowKey = row.getKey();
        for (Cell eachCell: row.getCellList(columnName)) {
          Cell.Key columnKey = eachCell.getKey();
          double value = Double.parseDouble(eachCell.getValue().getValueAsString());

          System.out.printf("%s, %s: %5.5f \n", rowKey.toString(), columnKey
              .toString(), value);
        }
      }
    } finally {
      scanner.close();
    }
  }

  public static void main(String[] args) throws Exception {
    if(args.length < 1) {
      System.out
      .println("Usage:java Matrix <op(data|cal)> <options>");
      System.exit(0);
    }
    
    if("data".equals(args[0])) {
      insertTestData(args);
    } else if ("cal".equals(args[0])) {
      calTestData(args);
    } else {
      System.out
      .println("Usage:java Matrix <op(data|cal)> <options>");
      System.exit(0);
    }
  }
  
  public static void insertTestData(String[] args) throws Exception {
    if (args.length < 4) {
      System.out
          .println("Usage:java Matrix <op(data|cal)> <InputTableName> <# Input rows> <#Input Cols>");
      System.exit(0);
    }
    
    int numOfRows = Integer.parseInt(args[2]);
    int numOfCols = Integer.parseInt(args[3]);
    String inputTableName = args[1];
    
    CloudataConf conf = new CloudataConf();

    boolean insert = !CTable.existsTable(conf, inputTableName);
    SparseMatrix matrix1 = new SparseMatrix(conf, inputTableName, "Col1");

    // if(insert) {
    // matrix1.addValue("a", "a", 1);
    // matrix1.addValue("a", "c", 1);
    // matrix1.addValue("b", "a", 1);
    // matrix1.addValue("b", "b", 1);
    // matrix1.addValue("b", "c", 1);
    // matrix1.addValue("c", "b", 1);
    // }

    matrix1.initUploader();
    DecimalFormat keyFormat = new DecimalFormat("00000000000");
    if (insert) {
      Random rand = new Random();
      for (int i = 1; i < numOfRows; i++) {
        for (int j = 0; j < numOfCols; j++) {
          matrix1.addToUploader(keyFormat.format(i), keyFormat.format(rand
              .nextInt(numOfRows) + 1), 1.0);
        }
        if (i % 1000 == 0) {
          LOG.info(i + " inserted");
        }
      }
    }
    matrix1.closeUploader();
    LOG.info("End uploading:" + numOfRows + " rows uploaded");
  }

  public static void calTestData(String[] args) throws Exception {
    if (args.length < 4) {
      System.out
          .println("Usage:java Matrix <op(data|cal)> <local(Y|N)> <OutputTableName> <InputTableName> [InputTabletName]");
      System.exit(0);
    }
    LOG.info("Start multiply:" + args[3] + "*" + args[3] + "=" + args[2]);
    
    boolean local = "Y".equals(args[1].toUpperCase());
    
    String inputTableName = args[3];
    
    CloudataConf conf = new CloudataConf();

    SparseMatrix matrix1 = new SparseMatrix(conf, inputTableName, "Col1");
    SparseMatrix matrix2 = new SparseMatrix(conf, inputTableName, "Col1");
    if(args.length > 4) {
      matrix1.setTabletName(args[4]);
    }

    String resultTableName = args[2];
    long startTime = System.currentTimeMillis();
    SparseMatrix resultMatrix = new SparseMatrix(conf, resultTableName, "Col1", matrix1);
    
    if(local) {
      matrix1.mutiplyLocal(matrix2, resultMatrix);
    } else {
      matrix1.mutiply(matrix2, resultMatrix);
    }

    LOG.info("End:" + (System.currentTimeMillis() - startTime));
    //resultMatrix.print();
  }
}
