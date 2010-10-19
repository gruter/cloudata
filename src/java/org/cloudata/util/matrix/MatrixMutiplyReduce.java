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
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.cloudata.core.common.conf.CloudataConf;


public class MatrixMutiplyReduce implements Reducer<WritableComparable, Writable, Text, Text> {
  IOException err;
  AbstractMatrix resultMatrix;
  Reporter reporter;
  public void reduce(WritableComparable key, Iterator<Writable> values, 
      OutputCollector<Text, Text> colletcor, Reporter reporter) throws IOException {
    if(this.reporter == null) {
      this.reporter = reporter;
    }
    MatrixItem matrixItem = (MatrixItem)key;
    double sum = 0;
    while(values.hasNext()) {
      Text value = (Text)values.next();
      sum += Double.parseDouble(value.toString());
    }
    resultMatrix.addToUploader(matrixItem.row, matrixItem.column, sum);
  }

  public void configure(JobConf job) {
    CloudataConf conf = new CloudataConf();
    boolean sparse = job.getBoolean(MatrixInputFormat.MATRIX_RESULT_SPARSE, false);
    String resultTableName = job.get(MatrixInputFormat.MATRIX_RESULT_TABLE);
    String resultColumnName = job.get(MatrixInputFormat.MATRIX_RESULT_COLUMN);
    
    try {
      if(sparse) {
        resultMatrix = new SparseMatrix(conf, resultTableName, resultColumnName);
      } else {
        resultMatrix = new Matrix(conf, resultTableName, resultColumnName);
      }
      resultMatrix.initUploader();
    } catch (IOException e) {
      err = e;
    }
  }

  public void close() throws IOException {
    resultMatrix.closeUploader();
  }
}
