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
package org.cloudata.core.parallel.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.RowFilter;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.tablet.TabletInfo;


/**
 * An InputFormat for Table. Table is splited by Tablet. Each InputSplit represents single Tablet.<BR>
 * Key is Row.Key, and value is Row(if RowScan is true) or ScanCell(if RowScan is false).   
 *
 */
public abstract class AbstractTabletInputFormat implements InputFormat, JobConfigurable {
  static final Logger LOG = Logger.getLogger(AbstractTabletInputFormat.class.getName());

  public static final String INPUT_TABLE = "AbstractTabletInputFormat.INPUT_TABLE";
  public static final String INPUT_COLUMN_LIST = "AbstractTabletInputFormat.INPUT_COLUMN_LIST";
  public static final String OUTPUT_TABLE = "AbstractTabletInputFormat.OUTPUT_TABLE";
  public static final String SKIP_ERROR = "cloudata.mapreduce.skipError";
  
  private CloudataConf conf;
  
  protected JobConf jobConf;
  
  public AbstractTabletInputFormat() throws IOException {
    conf = new CloudataConf();
  }

  public abstract InputTableInfo[] getInputTableInfos(JobConf jobConf); 
  
  @Override
  public RecordReader<Row.Key, Writable> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
    TableSplit tSplit = (TableSplit)split;
    
    InputTableInfo inputTableInfo = tSplit.getInputTableInfo();
    
    if(inputTableInfo.getJoinTableName() != null && inputTableInfo.getJoinTableName().length() > 0) {
      return new TableJoinRecordReader(job, conf, tSplit, reporter);
    } else {
      if(inputTableInfo.isRowScan()) {
        return new TableRowRecordReader(job, conf, tSplit, reporter);
      } else {
        return new TableScanCellReader(job, conf, tSplit, reporter);
      }
    }
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    InputTableInfo[] inputTableInfos = getInputTableInfos(job);
    
    if(inputTableInfos == null || inputTableInfos.length == 0) {
      throw new IOException("No input table infos. check getInputTableInfos() method");
    }
    
    List<TableSplit> result = new ArrayList<TableSplit>();
    for(InputTableInfo eachTableInfo: inputTableInfos) {
      result.addAll(getTableSplits(job, numSplits, eachTableInfo));
    }
    
    return result.toArray(new InputSplit[]{});
  }
  
  private List<TableSplit> getTableSplits(JobConf job, int numSplits, InputTableInfo inputTableInfo) throws IOException {
    CTable pivotTable = CTable.openTable(conf, inputTableInfo.getTableName());
    if(pivotTable == null) {
      throw new IOException("No table:" + inputTableInfo.getTableName());
    }
    TabletInfo[] tabletInfos = pivotTable.listTabletInfos();
    
    if(tabletInfos == null || tabletInfos.length == 0) {
      throw new IOException("Pivot table[" + pivotTable.getTableName() + "] has at least one tablet");
    }
    
    RowFilter rowFilter = inputTableInfo.getRowFilter();
    if(rowFilter == null) {
      rowFilter = new RowFilter();
    }
    
    List<TabletInfo> rangedTablets = new ArrayList<TabletInfo>();
    
    for(TabletInfo eachTablet:  tabletInfos) {
      if(eachTablet.getEndRowKey().compareTo(rowFilter.getStartRowKey()) < 0) {
        continue;
      }
      
      if(eachTablet.getStartRowKey().compareTo(rowFilter.getEndRowKey()) >= 0) {
        break;
      }
      
      LOG.info("Added:" + eachTablet);
      rangedTablets.add(eachTablet);
    }
    
    if(rangedTablets.size() == 0) {
      throw new IOException("No tablet in matched rowkey range: [" + 
          rowFilter.getStartRowKey() + "]~[" + rowFilter.getEndRowKey() + "]");
    }
    
    numSplits = 0;
    if(numSplits > rangedTablets.size()) {
      //사용자가 지정한 map 갯수가 tablet의 갯수보다 큰 경우
      int splitPerTablet = numSplits / rangedTablets.size();
      //InputSplit[] splits = new InputSplit[numSplits];
      List<TableSplit> splits = new ArrayList<TableSplit>();
      
      Row.Key startRowKey = rowFilter.getStartRowKey();
      
      for(TabletInfo eachTablet:  rangedTablets) {
        //FIXME getSplitedRowKeyRanges 호출 시 start, end 정보도 넘겨 줘야 함
        Row.Key[] rowKeys = pivotTable.getSplitedRowKeyRanges(eachTablet, splitPerTablet);
            //startRowKey, rowFilter.getEndRowKey());
        if(rowKeys == null) {
          LOG.info("No Split Data in " + eachTablet);
          continue;
        }
        for(int j = 0; j < rowKeys.length; j++) {
          RowFilter eachRowFilter = rowFilter.copyRowFilter();
          eachRowFilter.setStartRowKey(startRowKey);
          eachRowFilter.setEndRowKey(rowKeys[j]);

          TableSplit split = new TableSplit(inputTableInfo, eachTablet.getAssignedHostName(), eachRowFilter);
          split.setJobStartRowKey(rowFilter.getStartRowKey());
          
          splits.add(split);
          
          startRowKey = rowKeys[j];
          
          if(startRowKey.compareTo(rowFilter.getEndRowKey()) > 0) {
            break;
          }
        }
      }
      
      if(splits.size() > 0) {
        splits.get(splits.size() - 1).getRowFilter().setEndRowKey(rowFilter.getEndRowKey());
      }
      
//      if(splits.size() != numSplits) {
//        LOG.warn("Not matched splits.size():" + splits.size() + " and numSplits:" + numSplits);
//      }
      return splits;
    } else {
      //사용자가 지정한 map 갯수보다 tablet이 많은 경우 tablet 갯수로 job을 split
      List<TableSplit> splits = new ArrayList<TableSplit>();
      Row.Key startRowKey = rowFilter.getStartRowKey();
      for(TabletInfo eachTablet: rangedTablets) {
        RowFilter eachRowFilter = rowFilter.copyRowFilter();
        eachRowFilter.setStartRowKey(startRowKey);
        eachRowFilter.setEndRowKey(eachTablet.getEndRowKey());
        TableSplit split = new TableSplit(inputTableInfo, eachRowFilter);
        split.setJobStartRowKey(rowFilter.getStartRowKey());
        splits.add(split);
        startRowKey = eachTablet.getEndRowKey();
        if(startRowKey.compareTo(rowFilter.getEndRowKey()) > 0) {
          break;
        }
      }
      if(splits.size() > 0) {
        splits.get(splits.size() - 1).getRowFilter().setEndRowKey(rowFilter.getEndRowKey());
      }
      
      for(TableSplit eachSplit: splits) {
        LOG.info("AAAA:" + eachSplit.getRowFilter().getStartRowKey() + eachSplit.getRowFilter().getEndRowKey());
      }
      return splits;
    }    
  }

  @Override
  public void configure(JobConf jobConf) {
    this.jobConf = jobConf;
  }

  public void validateInput(JobConf job) throws IOException {
    InputTableInfo[] inputTableInfos = getInputTableInfos(job);
    if(inputTableInfos == null || inputTableInfos.length == 0) {
      throw new IOException("No InputTableInfo. check getInputTableInfos()");
    }
  }
}

