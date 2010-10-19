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
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import org.cloudata.core.common.conf.CloudataConf;


public class MatrixMutiplyMap implements Mapper<WritableComparable, Writable, MatrixItem, Text> {
  static final Logger LOG = Logger.getLogger(MatrixMutiplyMap.class.getName());
  //Column -> result
  HashMap<String, Double> calResult = new HashMap<String, Double>();
  String previousRow;
  int count = 1;
  AbstractMatrix targetMatrix;
  long startTime = System.currentTimeMillis();
  IOException err;
  OutputCollector collector;
  
  public void map(WritableComparable key, Writable value, 
      OutputCollector<MatrixItem, Text> collector, Reporter reporter) throws IOException {
    if(err != null) {
      throw err;
    }
    
    if(this.collector == null) {
      this.collector = collector;
    }
    
    MatrixItem matrixItem = (MatrixItem)value;
    String row = matrixItem.row;
    String column = matrixItem.column;

    // 다음번 row가 나타나면 이전에 처리하던 값은 저장한다.
    if (previousRow != null && !previousRow.equals(row)) {
      for (Map.Entry<String, Double> entry : calResult.entrySet()) {
        String targetCol = entry.getKey();
        Double resultValue = entry.getValue();

        MatrixItem resultItem = new MatrixItem();
        resultItem.row = previousRow;
        resultItem.column = targetCol;
        collector.collect(resultItem, new Text(resultValue.toString()));
      }
      // 초기화
      calResult = new HashMap<String, Double>();

      if(count % 100 == 0) {
        LOG.info(count + " rows completed: "
            + (System.currentTimeMillis() - startTime) + " ms, last row=" + previousRow);
        startTime = System.currentTimeMillis();
      }
      
      count++;
    }
    targetMatrix.initScannerForTarget(column);
    MatrixItem targetMatrixItem = null;
    try {
      while ((targetMatrixItem = targetMatrix.nextItemForTarget()) != null) {
        String targetCol = targetMatrixItem.column;
        double result = matrixItem.value * targetMatrixItem.value;

        if (calResult.containsKey(targetCol)) {
          calResult.put(targetCol, calResult.get(targetCol) + result);
        } else {
          calResult.put(targetCol, result);
        }
      }
    } finally {
      targetMatrix.closeScannerForTarget();
    }
    previousRow = row;
  }

  public void configure(JobConf job) {
    CloudataConf conf = new CloudataConf();
    boolean sparse = job.getBoolean(MatrixInputFormat.MATRIX_TARGET_SPARSE, false);
    String targetTableName = job.get(MatrixInputFormat.MATRIX_TARGET_TABLE);
    String targetColumnName = job.get(MatrixInputFormat.MATRIX_TARGET_COLUMN);
    
    try {
      if(sparse) {
        targetMatrix = new SparseMatrix(conf, targetTableName, targetColumnName);
      } else {
        targetMatrix = new Matrix(conf, targetTableName, targetColumnName);
      }
    } catch (IOException e) {
      err = e;
    }
  }

  public void close() throws IOException {
    // 마지막 row에 대한 처리
    if (calResult != null && calResult.size() > 0) {
      for (Map.Entry<String, Double> entry : calResult.entrySet()) {
        String targetCol = entry.getKey();
        Double resultValue = entry.getValue();

        MatrixItem resultItem = new MatrixItem();
        resultItem.row = previousRow;
        resultItem.column = targetCol;
        //collector.collect(resultItem, new Text(resultValue.toString()));
      }
    }
  }
}
