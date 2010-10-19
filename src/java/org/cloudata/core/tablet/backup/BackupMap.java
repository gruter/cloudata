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
package org.cloudata.core.tablet.backup;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.Row;


/**
 * cloudata에 저장된 데이터를 파일시스템으로 flat한 파일로 저장하는 MapReduce Job의 Mapper 클래스
 * 데이터는 다음과 같은 순서로 저장 CSV 형태로 저장: rowkey, columnName, columnkey, value, timestamp
 * @author jindolk
 *
 */
public class BackupMap implements Mapper<Row.Key, Row, Text, Text> {
  static final Log LOG = LogFactory.getLog(BackupMap.class.getName());
  static Text EMPTY = new Text("");
  long count = 0;
  
  @Override
  public void map(Row.Key key, Row value, OutputCollector<Text, Text> output,
      Reporter reporter) throws IOException {
    
    String[] columnNames = value.getColumnNames();
    if(columnNames == null || columnNames.length == 0) {
      return;
    }
    
    String rowKey = "\"" + key.toString() + "\",";
    for(String eachColumn: columnNames) {
      List<Cell> cells = value.getCellList(eachColumn);
      if(cells == null || cells.size() == 0) {
        continue;
      }
      
      //0-rowkey, 1-columnname, 2-cellkey, 3,4...- value
      for(Cell eachCell: cells) {
        String valueStr = rowKey + "\"" + eachColumn + "\",\"" + eachCell.getKey().toString() + "\"";
        List<Cell.Value> values = eachCell.getValues();
        if(values != null && values.size() > 0) {
          for(Cell.Value eachValue: values) {
            if(eachValue.getBytes() != null) {
              String eachValueStr = valueStr + ",\"" + eachValue.getValueAsString() + 
                                    "\",\"" + eachValue.getTimestamp() +
                                    "\",\"" + eachValue.isDeleted() + "\"";
              
              output.collect(new Text(eachValueStr), EMPTY);
              count++;
              if(count % 10000 == 0) {
                LOG.info(count + " cells stored");
              }
            }
          }
        } else {
          output.collect(new Text(valueStr), EMPTY);
          count++;
          if(count % 10000 == 0) {
            LOG.info(count + " cells stored");
          }
        }
      }
    }
  }

  @Override
  public void configure(JobConf job) {
  }

  @Override
  public void close() throws IOException {
    LOG.info("total " + count + " cells stored");
  }

}
