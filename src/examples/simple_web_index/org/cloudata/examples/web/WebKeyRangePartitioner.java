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
package org.cloudata.examples.web;


import java.io.UnsupportedEncodingException;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.parallel.hadoop.AbstractTabletInputFormat;
import org.cloudata.core.tablet.TabletInfo;


public class WebKeyRangePartitioner implements Partitioner<WritableComparable, Writable> {
  public static final Log LOG = LogFactory
      .getLog(WebKeyRangePartitioner.class.getName());

  private SortedSet<RowKeyItem> tabletInfoSet = new TreeSet<RowKeyItem>();
  private Exception confException;
  
  int count = 0;
  public int getPartition(WritableComparable key, Writable value, int numPartitions) {
    if(confException != null) {
      LOG.error(confException.getMessage(), confException);
      return -1;
    }

    if(numPartitions != tabletInfoSet.size()) {
      LOG.error("tablet count(" + tabletInfoSet.size() + ") not equals numPartitions (" + numPartitions + ")");
      return -1;
    }
    
    if(tabletInfoSet.size() == 0) {
      LOG.error("tablet partition size is zero");
      return -1;
    }
    int partitionNumber = 0;
    Text tKey = (Text)key;
    
    Row.Key rowKey;
    
    int keyIndex = tKey.find("\t");
    if(keyIndex < 0) {
      LOG.error("invalid value:" + tKey);
      rowKey = Row.Key.MAX_KEY;
    } else {
      rowKey = new Row.Key(tKey.getBytes(), 0, keyIndex);
    }
    
    SortedSet<RowKeyItem> tailSet = tabletInfoSet.tailSet(new RowKeyItem(rowKey, 0));
    RowKeyItem item = null;
    if(tailSet.size() > 0) {
      item = tailSet.first();
      partitionNumber = item.index;
    } else {
      item = tabletInfoSet.last();
      partitionNumber = item.index;
    }
    
    if(partitionNumber >= numPartitions) {
      LOG.info("Partition Number is : " + partitionNumber
      + ", numPartitions : " + numPartitions
      + ", Row.Key : " + key.toString());
      partitionNumber = numPartitions - 1;
    }
    //LOG.info("tablet partition num:" + partitionNumber);
    count++;
    if(count % 5000 == 0) {
      try {
        System.out.println("Partitioned:" + new String(rowKey.getBytes(), "EUC-KR") + "," + 
            new String(item.rowKey.getBytes(), "EUC-KR"));
      } catch (UnsupportedEncodingException e) {
        
      }
    }
    return partitionNumber;
  }

  public void configure(JobConf jobConf) {
    String tableName = jobConf.get(AbstractTabletInputFormat.OUTPUT_TABLE);
    try {
      CTable ctable = CTable.openTable(new CloudataConf(), tableName);
      TabletInfo[] tabletInfos = ctable.listTabletInfos();
      if(tabletInfos == null) {
        confException = new Exception("No Tablets in table [" + tableName + "]");
        LOG.error("No Tablets in table [" + tableName + "]");
        return;
      }
      for(int i = 0; i < tabletInfos.length; i++) {
        LOG.info("Add endRowKey: " + new String(tabletInfos[i].getEndRowKey().getBytes(), "EUC-KR"));
        tabletInfoSet.add(new RowKeyItem(tabletInfos[i].getEndRowKey(), i));
      }
      if(tabletInfoSet.size() == 0) {
        confException = new Exception("No Key raneg in table [" + tableName + "]");
        LOG.error("No Key raneg in table [" + tableName + "]");
      }
    } catch (Exception e) {
      LOG.error("KeyRangePartitioner config error:" + tableName + "," + e.getMessage());
      e.printStackTrace(System.out);
      confException = e;
    }
  }
  
  static class RowKeyItem implements Comparable<RowKeyItem> {
    Row.Key rowKey;
    int index;
    
    RowKeyItem(Row.Key rowKey, int index) {
      this.rowKey = rowKey;
      this.index = index;
    }

    public int compareTo(RowKeyItem item) {
      return rowKey.compareTo(item.rowKey);
    }
  }
}
