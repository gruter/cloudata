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
package org.cloudata.core.tabletserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.cloudata.core.client.CellFilter;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.CTableManager;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.tablet.ColumnValue;
import org.cloudata.core.tablet.TabletInfo;


/**
 * @author jindolk
 *
 */
public class ColumnMemoryCache {
  long memorySize;
  ColumnCollection columnCollection;
  final ReentrantReadWriteLock columnCollectionsLock = new ReentrantReadWriteLock();
  
  public ColumnMemoryCache(ColumnCollection columnCollection) {
    this.columnCollection = columnCollection;
    this.memorySize = columnCollection.getMemorySize();
  }

  public void addColumnCollection(ColumnCollection columnCollection) {
    columnCollectionsLock.writeLock().lock();
    try {
      this.columnCollection.columnCollectionMap.putAll(columnCollection.columnCollectionMap);
      this.memorySize += columnCollection.getMemorySize();
    } finally {
      columnCollectionsLock.writeLock().unlock();
    }
  }
  
  public long getMemorySize() {
    return memorySize;
  }
  
  public ColumnMemoryCacheSearcher getSearcher(Row.Key rowKey, CellFilter cellFilter) throws IOException {
    return new ColumnMemoryCacheSearcher(rowKey, cellFilter);
  }
  
  public void clear() {
    columnCollectionsLock.writeLock().lock();
    try {
      columnCollection.columnCollectionMap.clear();
      columnCollection = null;
    } finally {
      columnCollectionsLock.writeLock().unlock();
    }
  }
  
  public ColumnValue findClosest(Row.Key rowKey) {
    columnCollectionsLock.readLock().lock();
    try {
      return columnCollection.findNearestValue(rowKey);
    } finally {
      columnCollectionsLock.readLock().unlock();
    }
  }
  
  class ColumnMemoryCacheSearcher implements Searchable {
    Iterator<ColumnValue> columnValueIt;
    List<ColumnValue> columnValues;

    public ColumnMemoryCacheSearcher(Row.Key rowKey, CellFilter cellFilter) throws IOException {
      synchronized(columnCollection) {
        columnValues = new ArrayList<ColumnValue>(10);
        columnCollectionsLock.readLock().lock();
        try {
          columnCollection.search(rowKey, cellFilter, columnValues);
        } finally {
          columnCollectionsLock.readLock().unlock();
        }
        columnValueIt = columnValues.iterator();
      }
    }
  
    public ColumnValue next() throws IOException {
      if (columnValueIt == null || !columnValueIt.hasNext()) {
        columnValueIt = null;
        return null;
      }
  
      ColumnValue result = columnValueIt.next();
      return result;
    }
  
    public void close() throws IOException {
      columnValueIt = null;
      columnValues = null;
    }
  }
  
  public static void main(String[] args) throws Exception {
    if(args.length < 3) {
      System.out.println("Usage: java ColumnMemoryCache <table name> <tablet name> <column name>");
      System.exit(0);
    }
    
    String tableName = args[0];
    String tabletName = args[1];
    String columnName = args[2];
    
    CloudataConf conf = new CloudataConf();
    
    CTable ctable = CTable.openTable(conf, tableName);
    
    TabletInfo tabletInfo = ctable.getTabletInfo(tabletName);
    
    DataServiceProtocol tabletServer = CTableManager.connectTabletServer(tabletInfo, conf);
    
    ColumnValue[] columnValues = tabletServer.getColumnMemoryCacheDatas(tabletName, columnName);
    if(columnValues == null) {
      System.out.println("No data");
    } else {
      for(ColumnValue eachColumnValue: columnValues) {
        System.out.println(eachColumnValue.toString());
      }
    }
  }
}
