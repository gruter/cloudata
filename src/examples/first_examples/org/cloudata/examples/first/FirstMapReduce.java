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
package org.cloudata.examples.first;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CellFilter;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.RowFilter;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.parallel.hadoop.AbstractTabletInputFormat;
import org.cloudata.core.parallel.hadoop.InputTableInfo;
import org.cloudata.core.parallel.hadoop.CloudataMapReduceUtil;
import org.cloudata.core.tablet.TableSchema;
import org.cloudata.core.tablet.TabletInfo;


/**
 * Cloudata의 테이블 데이터를 이용하여 Hadoop MapReduce Job을 실행하는 간단한 예제
 *
 */
public class FirstMapReduce {
  public static void main(String[] args) throws Exception {
    //Output 테이블 생성
    CloudataConf conf = new CloudataConf();
    String outputTableName = "InvertedTable";
    TableSchema outputTableSchema = new TableSchema();
    outputTableSchema.addColumn("InvertedCloumn");
    if(!CTable.existsTable(conf, outputTableName)) {
      CTable.createTable(conf, outputTableSchema);
    }  
    
    JobConf jobConf = new JobConf(FirstMapReduce.class);
    jobConf.setJobName("FirstMapReduce");
    String libDir = CloudataMapReduceUtil.initMapReduce(jobConf);
    
    //<Mapper>
    //Mapper 클래스 지정
    jobConf.setMapperClass(FirstMapReduceMapper.class);
    //InputFormat을 TabletInputFormat으로 지정
    jobConf.setInputFormat(FirstMapReduceInputFormat.class);
    jobConf.setMapOutputKeyClass(Text.class);
    jobConf.setMapOutputValueClass(Text.class);
    //</Mapper>
    
    //<Reducer>
    String outputPath = "temp/FirstMapReduce";
    FileOutputFormat.setOutputPath(jobConf, new Path(outputPath));
    //Reducer 클래스 지정
    jobConf.setReducerClass(FirstMapReduceReducer.class);
    jobConf.setOutputKeyClass(Text.class);
    jobConf.setOutputValueClass(Text.class);
    //Map갯수를 Reduce 갯수와 동일하게 한다.
    CTable ctable = CTable.openTable(conf, "SampleTable1");
    TabletInfo[] tabletInfos = ctable.listTabletInfos();
    jobConf.setNumReduceTasks(tabletInfos.length);
    //Reduce의 경우 Tablet에 데이터를 저장하는 작업을 수행하기 때문에
    //Task 실패 시 반복하지 않도록 재시도 횟수를 0으로 설정한다.
    jobConf.setMaxReduceAttempts(0);
    //</Reducer>

    try {
      //Job 실행
      JobClient.runJob(jobConf);
    } finally {
      //Temp 삭제
      FileSystem fs = FileSystem.get(jobConf);
      fs.delete(new Path(outputPath), true);
      CloudataMapReduceUtil.clearMapReduce(libDir);
    }
  }

  static class FirstMapReduceInputFormat 
      extends AbstractTabletInputFormat {
    public FirstMapReduceInputFormat() throws IOException {
      super();
    }

    @Override
    public InputTableInfo[] getInputTableInfos(JobConf jobConf) {
      RowFilter rowFilter = new RowFilter();
      rowFilter.addCellFilter(new CellFilter("Column1"));
      
      InputTableInfo inputTableInfo = new InputTableInfo();
      inputTableInfo.setTable("SimpleTable", rowFilter);
      //inputTableInfo.setRowScan(false);
      
      return new InputTableInfo[]{inputTableInfo};
    }
  }
  
  static class FirstMapReduceMapper 
      implements Mapper<Row.Key, Row, Text, Text> {
    @Override
    public void map(Row.Key key, Row value, OutputCollector<Text, Text> output,
        Reporter reporter) throws IOException {
      for(Cell eachCell: value.getFirstColumnCellList()) {
        output.collect(new Text(eachCell.getKey().toString()), new Text(key.toString()));
      }
    }

    @Override
    public void configure(JobConf job) {
    }

    @Override
    public void close() throws IOException {
    }
    
  }
  
  static class FirstMapReduceReducer 
      implements Reducer<Text, Text, Text, Text> {
    CTable ctable;
    IOException exception;

    @Override
    public void reduce(Text key, Iterator<Text> values,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
      if(exception != null) {
        throw exception;
      }
      Row.Key rowKey = new Row.Key(key.toString());
      
      Row row = new Row(rowKey);
      while(values.hasNext()) {
        Text text = values.next();
        row.addCell("InvertedColumn", 
            new Cell(new Cell.Key(text.toString()), null));
      }
      
      ctable.put(row);
    }

    @Override
    public void configure(JobConf job) {
      try {
        CloudataConf conf = new CloudataConf();
        ctable = CTable.openTable(conf, "InvertedTable");
        if(ctable == null) {
          throw new IOException("No InvertedTable table");
        }
      } catch (IOException e) {
        exception = e;
      }
    }

    @Override
    public void close() throws IOException {
    }
  }
}
