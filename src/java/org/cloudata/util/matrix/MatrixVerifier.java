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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.common.conf.CloudataConf;


public class MatrixVerifier {
  private static final Log LOG = LogFactory.getLog(MatrixVerifier.class
      .getName());
  
  static HashMap<MatrixItem, Double> resultInCloudata = new HashMap<MatrixItem, Double>();
  static HashMap<MatrixItem, Double> resultInMemory = new HashMap<MatrixItem, Double>();

  public static void main(String[] args) throws Exception {
    if(args.length < 4) {
      System.out.println("Usage: java MatrixVerifier <input table name,column name> <target table name,column name> <result table name,column name> <sparseYn(Y|N)");
      System.exit(0);
    }
    CloudataConf conf = new CloudataConf();
    String inputTable = args[0];
    String targetTable = args[1];
    String resultTable = args[2];
    boolean sparse = "Y".equals(args[3].toUpperCase());
    
    AbstractMatrix inputMatrix = null;
    AbstractMatrix targetMatrix = null;
    AbstractMatrix resultMatrix = null;
    
    if(sparse) {
      inputMatrix = new SparseMatrix(conf, inputTable.split(",")[0], inputTable.split(",")[1]);
      targetMatrix = new SparseMatrix(conf, targetTable.split(",")[0], targetTable.split(",")[1]);
      resultMatrix = new SparseMatrix(conf, resultTable.split(",")[0], resultTable.split(",")[1]);
    } else {
      inputMatrix = new Matrix(conf, inputTable.split(",")[0], inputTable.split(",")[1]);
      targetMatrix = new Matrix(conf, targetTable.split(",")[0], targetTable.split(",")[1]);
      resultMatrix = new Matrix(conf, resultTable.split(",")[0], resultTable.split(",")[1]);
    }
    
    LOG.info("Start loading result table: " + resultTable);
    loadResultTable(resultMatrix);
    LOG.info("Start multifling in memory: " + inputTable + " * "  + resultTable);
    multiflyUsingMemory(inputMatrix, targetMatrix);
    LOG.info("Start comparing memory vs. cloudata");
    compare();
  }
  
  public static void multiflyUsingMemory(AbstractMatrix inputMatrix, AbstractMatrix targetMatrix) throws Exception {
    //row -> (column -> value)
    Map<String, Map<String, Double>> inputDatas = new HashMap<String, Map<String, Double>>();
    Map<String, Map<String, Double>> targetDatas = new HashMap<String, Map<String, Double>>();
    
    MatrixItem item = null;
    
    inputMatrix.initScanner();
    
    String previousRow = null;
    Map<String, Double> columnMap = new HashMap<String, Double>();
    while( (item = inputMatrix.nextItem()) != null ) {
      if(previousRow != null && !previousRow.equals(item.getRow())) {
        inputDatas.put(previousRow, columnMap);
        columnMap = new HashMap<String, Double>();
      }
      columnMap.put(item.column, item.value);
      previousRow = item.row;
    }
    inputMatrix.closeScanner();
    ///////////////////////////////////////////////////////
    
    targetMatrix.initScanner();
    item = null;
    previousRow = null;
    columnMap = new HashMap<String, Double>();
    while( (item = targetMatrix.nextItem()) != null ) {
      if(previousRow != null && !previousRow.equals(item.getRow())) {
        targetDatas.put(previousRow, columnMap);
        columnMap = new HashMap<String, Double>();
      }
      columnMap.put(item.column, item.value);
      previousRow = item.row;
    }
    targetMatrix.closeScanner();
    ///////////////////////////////////////////////////////
    
    for(Map.Entry<String, Map<String, Double>> entry: inputDatas.entrySet()) {
      String row = entry.getKey();
      Map<String, Double> columns = entry.getValue();
      
      for(Map.Entry<String, Double> columnEntry: columns.entrySet()) {
        String column = columnEntry.getKey();
        double value = columnEntry.getValue();
        
        Map<String, Double> targetRow = targetDatas.get(column);
        for(Map.Entry<String, Double> targetColumnEntry : targetRow.entrySet()) {
          String targetColumn = targetColumnEntry.getKey();
          double targetValue = targetColumnEntry.getValue();
          
          double result = value * targetValue;
          
          MatrixItem resultItem = new MatrixItem();
          resultItem.row = row;
          resultItem.column = targetColumn;
          if(resultInMemory.containsKey(resultItem)) {
            result = resultInMemory.get(resultItem) + result;
          } 
          resultInMemory.put(resultItem, result);
        }
      }
    }
  }
  
  public static void loadResultTable(AbstractMatrix resultMatrix) throws Exception {
    MatrixItem item = null;
    
    resultMatrix.initScanner();
    int count = 0;
    while( (item = resultMatrix.nextItem()) != null ) {
      resultInCloudata.put(item, item.value);
      count++;
    }
    resultMatrix.closeScanner();
  }
  
  public static void compare() throws Exception {
    for(Map.Entry<MatrixItem, Double> entry: resultInMemory.entrySet()) {
      MatrixItem memoryItem = entry.getKey();
      double memoryValue = entry.getValue();
      
      if(resultInCloudata.containsKey(memoryItem)) {
        double cloudataValue = resultInCloudata.get(memoryItem);
        if(memoryValue != cloudataValue) {
          LOG.info("No Matched: [" + memoryItem.row + "," + memoryItem.column + "=" + memoryValue + "],cloudata=" + cloudataValue);
          System.exit(0);
        }
      } else {
        LOG.info("No Cloudata Result:" + memoryItem);
        System.exit(0);
      }
    }
    
    LOG.info("All items machted");
  }
}
