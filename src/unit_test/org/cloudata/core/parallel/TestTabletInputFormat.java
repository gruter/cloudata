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
package org.cloudata.core.parallel;

import java.io.IOException;
import java.text.DecimalFormat;

import junit.framework.TestCase;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CellFilter;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.RowFilter;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.parallel.hadoop.AbstractTabletInputFormat;
import org.cloudata.core.parallel.hadoop.InputTableInfo;
import org.cloudata.core.parallel.hadoop.TableSplit;
import org.cloudata.core.tablet.TableSchema;


/**
 * @author jindolk
 *
 */
public class TestTabletInputFormat extends TestCase {
  String tableName = "T_INPUTFORMAT";
  
  public void testTabletInputFormat() throws IOException {
    CloudataConf conf = new CloudataConf();
    
    if(CTable.existsTable(conf, tableName)) {
      CTable.dropTable(conf, tableName);
    }
    TableSchema tableSchema = new TableSchema(tableName, "Test", new String[]{"Col1"});
    CTable.createTable(conf, tableSchema);
    
    CTable ctable = CTable.openTable(conf, tableName);
    
    DecimalFormat df = new DecimalFormat("0000000000");
    for(int i = 0; i < 20; i++) {
      Row row = new Row(new Row.Key("guest.sample.test1_" + df.format(i)));
      row.addCell("Col1", new Cell(new Cell.Key("xml"), "This is test".getBytes()));
      ctable.put(row);
    }

    for(int i = 0; i < 100; i++) {
      Row row = new Row(new Row.Key("guest.sample.test2_" + df.format(i)));
      row.addCell("Col1", new Cell(new Cell.Key("xml"), "This is test".getBytes()));
      ctable.put(row);
    }

    for(int i = 0; i < 20; i++) {
      Row row = new Row(new Row.Key("guest.sample.test3_" + df.format(i)));
      row.addCell("Col1", new Cell(new Cell.Key("xml"), "This is test".getBytes()));
      ctable.put(row);
    }

    System.out.println("================== 140 rows added ====================");
    
    JobConf jobConf = new JobConf();
    
    int count = 0;
    SampleTabletInputFormat inputFormat = new SampleTabletInputFormat();
    InputSplit[] inputSplits = inputFormat.getSplits(jobConf, 4);
    for(int i = 0; i < inputSplits.length; i++) {
      RecordReader<Row.Key, Writable> record = inputFormat.getRecordReader(inputSplits[i], jobConf, null);
      Row.Key key = record.createKey();
      Writable value = record.createValue();
      
      int readerIndex = 0;
      while(record.next(key, value)) {
        if(count == 0) {
          assertEquals("guest.sample.test2_" + df.format(i), key.toString());
        } else if(readerIndex == 0) {
          assertTrue(key.compareTo(((TableSplit)inputSplits[i]).getRowFilter().getStartRowKey()) > 0);
        }
        count++;
        readerIndex++;
        System.out.println(">>>>" + key);
      }
      if(i < inputSplits.length - 1) {
        assertEquals(((TableSplit)inputSplits[i]).getRowFilter().getEndRowKey(), key);
      } else {
        assertEquals("guest.sample.test2_" + df.format(99), key.toString());
      }
    }
    
    assertEquals(100, count);
  }
  
  class SampleTabletInputFormat extends AbstractTabletInputFormat {
    public SampleTabletInputFormat() throws IOException {
      super();
    }
  
    @Override
    public InputTableInfo[] getInputTableInfos(JobConf jobConf) {
      Row.Key rowKey = new Row.Key("guest.sample.test2_");
      RowFilter rowFilter = new RowFilter(rowKey, RowFilter.OP_LIKE);
      rowFilter.addCellFilter(new CellFilter("Col1", new Cell.Key("xml")));
        
        InputTableInfo inputTableInfo = new InputTableInfo();
        inputTableInfo.setTable(tableName, rowFilter);
      
      return new InputTableInfo[]{inputTableInfo};
    }
  }
}
