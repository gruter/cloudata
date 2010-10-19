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

import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.tablet.TabletInfo;

/**
 * KeyRangePartitioner partitions Key by tablet.<BR>
 * You must set AbstractTabletInputFormat.OUTPUT_TABLE and number of reduce task with number of AbstractTabletInputFormat.OUTPUT_TABLE's tablet   
 *
 */
public class KeyRangePartitioner implements Partitioner<WritableComparable, Writable> {
  public static final Log LOG = LogFactory
      .getLog(KeyRangePartitioner.class.getName());

  private SortedSet<RowKeyItem> tabletInfoSet = new TreeSet<RowKeyItem>();
  private Exception confException;
  
  int count = 0;
  @Override
  public int getPartition(WritableComparable key, Writable value,
      int numPartitions) {
    if(confException != null) {
      LOG.error(confException.getMessage(), confException);
      return -1;
    }

    if(tabletInfoSet.isEmpty()) {
      LOG.error("tablet partition size is zero");
      return -1;
    }
    int partitionNumber = 0;
    Row.Key rowKey = new Row.Key(key.toString());
    
    SortedSet<RowKeyItem> tailSet = tabletInfoSet.tailSet(new RowKeyItem(rowKey, 0));
    if(!tailSet.isEmpty()) {
      partitionNumber = tailSet.first().index;
    } else {
      partitionNumber = tabletInfoSet.last().index;
    }
    
    if(partitionNumber >= numPartitions) {
      LOG.info("Partition Number is : " + partitionNumber
      + ", numPartitions : " + numPartitions
      + ", Row.Key : " + key.toString());
      partitionNumber = numPartitions - 1;
    }

    count++;
    return partitionNumber;
  }

  @Override
  public void configure(JobConf jobConf) {
    String tableName = jobConf.get(AbstractTabletInputFormat.OUTPUT_TABLE);
    int numReduce = jobConf.getNumReduceTasks();
    try {
      CTable ctable = CTable.openTable(new CloudataConf(jobConf), tableName);
      TabletInfo[] tabletInfos = ctable.listTabletInfos();
      if(tabletInfos == null) {
        confException = new Exception("No Tablets in table [" + tableName + "]");
        LOG.error("No Tablets in table [" + tableName + "]");
        return;
      }
      if(numReduce > tabletInfos.length) {
        for(int i = 0; i < tabletInfos.length; i++) {
          //LOG.info("Add endRowKey: " + new String(tabletInfos[i].getEndRowKey().getBytes(), "EUC-KR"));
          tabletInfoSet.add(new RowKeyItem(tabletInfos[i].getEndRowKey(), i));
        }
      } else {
        int modValue = tabletInfos.length / numReduce;
        int partition = 0;
        for(int i = 0; i < tabletInfos.length; i++) {
          if(i % modValue == 0) {
            //LOG.info("Add endRowKey: " + new String(tabletInfos[i].getEndRowKey().getBytes(), "EUC-KR"));
            tabletInfoSet.add(new RowKeyItem(tabletInfos[i].getEndRowKey(), partition));
            partition++;
          }
        }
      }
      if(tabletInfoSet.isEmpty()) {
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
      //System.out.println(">>>>" + rowKey + ">" + item.rowKey + ">" + rowKey.compareTo(item.rowKey));
      return rowKey.compareTo(item.rowKey);
    }
  }
  
  public static void main(String[] args) throws Exception {
//    KeyRangePartitioner partitioner = new KeyRangePartitioner();
//    
//    partitioner.tabletInfoSet.add(new RowKeyItem(new Row.Key("-0"), 0));
//    partitioner.tabletInfoSet.add(new RowKeyItem(new Row.Key("-2"), 1));
//    partitioner.tabletInfoSet.add(new RowKeyItem(new Row.Key("-4"), 2));
//    partitioner.tabletInfoSet.add(new RowKeyItem(new Row.Key("-6"), 3));
//    partitioner.tabletInfoSet.add(new RowKeyItem(new Row.Key("-8"), 4));
//    partitioner.tabletInfoSet.add(new RowKeyItem(new Row.Key("1"), 5));
//    partitioner.tabletInfoSet.add(new RowKeyItem(new Row.Key("3"), 6));
//    partitioner.tabletInfoSet.add(new RowKeyItem(new Row.Key("5"), 7));
//    partitioner.tabletInfoSet.add(new RowKeyItem(new Row.Key("7"), 8));
//    partitioner.tabletInfoSet.add(new RowKeyItem(new Row.Key("9"), 9));
//    
//    Text key = new Text("-66342949971773054188");
//    Text value = new Text();
//    System.out.println(key + ":" + partitioner.getPartition(key, value, 10));
//    
//    
//    SortedSet<RowKeyItem> set = new TreeSet<RowKeyItem>();
//    set.add(new RowKeyItem(new Row.Key("-0"), 0));
//    set.add(new RowKeyItem(new Row.Key("-2"), 1));
//    set.add(new RowKeyItem(new Row.Key("-4"), 2));
//    set.add(new RowKeyItem(new Row.Key("-6"), 3));
//    set.add(new RowKeyItem(new Row.Key("-8"), 4));
//    set.add(new RowKeyItem(new Row.Key("1"), 5));
//    set.add(new RowKeyItem(new Row.Key("3"), 6));
//    set.add(new RowKeyItem(new Row.Key("5"), 7));
//    set.add(new RowKeyItem(new Row.Key("7"), 8));
//    set.add(new RowKeyItem(new Row.Key("9"), 9));
//    
//    SortedSet<RowKeyItem> aaa = set.tailSet(new RowKeyItem(new Row.Key("-66342949971773054188"), 0));
//    System.out.println("first>>>>" + aaa.first().rowKey);
//    System.out.println("last>>>>" + aaa.last().rowKey);
  }
}
