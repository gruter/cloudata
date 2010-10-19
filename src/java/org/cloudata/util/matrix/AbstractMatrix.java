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
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.cloudata.core.client.DirectUploader;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.parallel.hadoop.AbstractTabletInputFormat;
import org.cloudata.core.parallel.hadoop.KeyRangePartitioner;
import org.cloudata.core.tablet.TableSchema;
import org.cloudata.core.tablet.TabletInfo;


public abstract class AbstractMatrix {
  private static final Log LOG = LogFactory.getLog(AbstractMatrix.class
      .getName());

  // public static final String VALUE_COLUMN = "m_value";
  public static final DecimalFormat KEY_FORMAT = new DecimalFormat(
      "00000000000");

  public static final String MAX_KEY_VALUE = "99999999999";

  // private String name;
  protected CTable ctable;

  protected String columnName;

  protected CloudataConf conf;

  protected DirectUploader uploader;

  public AbstractMatrix(CloudataConf conf, String tableName,
      String columnName) throws IOException {
    this(conf, tableName, columnName, null);
  }
  
  public AbstractMatrix(CloudataConf conf, String tableName,
      String columnName, AbstractMatrix inputMatrix) throws IOException {
    this.conf = conf;
    this.columnName = columnName;
    if (!CTable.existsTable(conf, tableName)) {
      TableSchema tableSchema = new TableSchema(tableName);
      tableSchema.addColumn(columnName);
      tableSchema.setNumOfVersion(3);
      
      if(inputMatrix != null) {
        TabletInfo[] tabletInfos = inputMatrix.ctable.listTabletInfos();
        Row.Key[] rowKeys = new Row.Key[tabletInfos.length];
        int index = 0;
        for(TabletInfo eachTabletInfo: tabletInfos) {
          rowKeys[index++] = eachTabletInfo.getEndRowKey();
        }
        CTable.createTable(conf, tableSchema, rowKeys);
      } else {
        CTable.createTable(conf, tableSchema);
      }
    }
    ctable = CTable.openTable(conf, tableName);
  }

  public abstract void addValue(String row, String col, double value)
      throws IOException;

  public abstract void addToUploader(String row, String col, double value)
      throws IOException;

  protected abstract MatrixItem nextItem() throws IOException;

  protected abstract void initScanner() throws IOException;

  protected abstract void closeScanner() throws IOException;

  protected abstract void initScannerForTarget(String row) throws IOException;

  protected abstract void closeScannerForTarget() throws IOException;

  protected abstract MatrixItem nextItemForTarget() throws IOException;

  protected abstract boolean isSparse() throws IOException;

  protected abstract void print() throws IOException;

  public void initUploader() throws IOException {
    if (uploader != null) {
      throw new IOException("Already uploader opened");
    }

    uploader = ctable.openDirectUploader(new String[] { columnName });
  }

  public void closeUploader() throws IOException {
    if (uploader == null) {
      throw new IOException("Not uploader opened");
    }

    uploader.close();
    uploader = null;
  }

  public void mutiply(AbstractMatrix targetMatrix, AbstractMatrix resultMatrix)
      throws IOException {
    Path tempOutputPath = new Path("temp/Matrix_" + System.currentTimeMillis());
    
    JobConf jobConf = new JobConf(AbstractMatrix.class);
    jobConf.setJobName("Matrix_Mutiply_Job" + "(" + new Date() + ")");
    
    //<MAP>
    jobConf.setMapperClass(MatrixMutiplyMap.class);
    jobConf.setInputFormat(MatrixInputFormat.class);
    jobConf.set(MatrixInputFormat.MATRIX_INPUT_TABLE, ctable.getTableName());
    jobConf.set(MatrixInputFormat.MATRIX_INPUT_COLUMN, columnName);
    jobConf.set(MatrixInputFormat.MATRIX_TARGET_TABLE, targetMatrix.ctable.getTableName());
    jobConf.set(MatrixInputFormat.MATRIX_TARGET_COLUMN, targetMatrix.columnName);
    jobConf.setBoolean(MatrixInputFormat.MATRIX_TARGET_SPARSE, targetMatrix.isSparse());
    jobConf.setMapOutputKeyClass(MatrixItem.class);
    jobConf.setMapOutputValueClass(Text.class);    
    //</MAP>

    //<REDUCE>
    jobConf.setPartitionerClass(KeyRangePartitioner.class);
    jobConf.set(AbstractTabletInputFormat.OUTPUT_TABLE, resultMatrix.ctable.getTableName()); 
    jobConf.setReducerClass(MatrixMutiplyReduce.class);
    jobConf.set(MatrixInputFormat.MATRIX_RESULT_TABLE, resultMatrix.ctable.getTableName());
    jobConf.set(MatrixInputFormat.MATRIX_RESULT_COLUMN, resultMatrix.columnName);
    jobConf.setBoolean(MatrixInputFormat.MATRIX_RESULT_SPARSE, resultMatrix.isSparse());
    jobConf.setOutputKeyClass(Text.class);
    jobConf.setOutputValueClass(Text.class);   

    TabletInfo[] tabletInfos = resultMatrix.ctable.listTabletInfos();
    
    jobConf.setNumReduceTasks(tabletInfos.length);
    jobConf.setMaxReduceAttempts(0);
    FileOutputFormat.setOutputPath(jobConf, tempOutputPath);
    //</REDUCE>
    
    //Run Job
    JobClient.runJob(jobConf);
    
    //delete temp output path
    FileSystem fs = FileSystem.get(jobConf);
    fs.delete(tempOutputPath, true);
  }

  public void mutiplyLocal(AbstractMatrix targetMatrix,
      AbstractMatrix resultMatrix) throws IOException {
    initScanner();

    //resultMatrix.initUploader();

    LOG.info("============== multiply ================");
    try {
      int count = 0;

      MatrixItem matrixItem = null;

      String previousRow = null;
      // Column -> result
      HashMap<String, Double> calResult = new HashMap<String, Double>();

      long startTime = System.currentTimeMillis();
      while ((matrixItem = nextItem()) != null) {
        String row = matrixItem.row;
        String column = matrixItem.column;

        // 다음번 row가 나타나면 이전에 처리하던 값은 저장한다.
        if (previousRow != null && !previousRow.equals(row)) {
          for (Map.Entry<String, Double> entry : calResult.entrySet()) {
            String targetCol = entry.getKey();
            Double resultValue = entry.getValue();

            //resultMatrix.addToUploader(previousRow, targetCol, resultValue);
          }
          // 초기화
          calResult = new HashMap<String, Double>();

          count++;
          if(count % 100 == 0) {
            LOG.info(count + " rows completed: "
                + (System.currentTimeMillis() - startTime) + " ms, last row=" + previousRow);
            startTime = System.currentTimeMillis();
          }
        }
        targetMatrix.initScannerForTarget(column);
        MatrixItem targetMatrixItem = null;
        try {
          // System.out.println("left>>>>" + matrixItem);
          while ((targetMatrixItem = targetMatrix.nextItemForTarget()) != null) {
            // System.out.println("right>>>>" + targetMatrixItem);
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
      }// end of while(scan)

      // 마지막 row에 대한 처리
      if (calResult != null && calResult.size() > 0) {
        for (Map.Entry<String, Double> entry : calResult.entrySet()) {
          String targetCol = entry.getKey();
          Double resultValue = entry.getValue();

          //resultMatrix.addToUploader(previousRow, targetCol, resultValue);
        }
      }
    } finally {
      try {
        closeScanner();
      } catch (Exception e) {
        e.printStackTrace();
      }

      try {
        //resultMatrix.closeUploader();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
