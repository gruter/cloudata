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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CellFilter;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.Row.Key;
import org.cloudata.core.common.CStopWatch;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.fs.CloudataFileSystem;
import org.cloudata.core.fs.GPath;
import org.cloudata.core.tablet.ColumnValue;
import org.cloudata.core.tablet.TabletInfo;
import org.cloudata.core.tabletserver.TabletMapFile.MapFileWriter;


public class ColumnCollection {
  public static final Log LOG = LogFactory.getLog(ColumnCollection.class.getName());
  
  protected CloudataConf conf;
  
  //rowkey, <columnkey, ValueCollection>, ValueCollection = value + timestamp
  protected ReentrantReadWriteLock mapLock = new ReentrantReadWriteLock();
  protected SortedMap<Row.Key, SortedMap<Cell.Key, ValueCollection>> columnCollectionMap 
          = new TreeMap<Row.Key, SortedMap<Cell.Key, ValueCollection>>();
  
  Row.Key leftLast = null;
  Row.Key rightFirst = null;

  long memorySize;
  
  public ColumnCollection(CloudataConf conf) {
    this.conf = conf;
  }

  public int addValue(Row.Key rowKey, ColumnValue columnValue, int numOfVersion) throws IOException {
    int saveSize = 0;
    int rowKeyLength = rowKey.getLength();
    int rowKeyAllocLen = rowKey.getAllocatedSize();
    
    SortedMap<Cell.Key, ValueCollection> columnValues = null;
    mapLock.readLock().lock();
    try {
      columnValues = columnCollectionMap.get(rowKey); 
    } finally {
      mapLock.readLock().unlock();
    }
    
    if(columnValues == null) {
      mapLock.writeLock().lock();
      try {
        if ((columnValues = columnCollectionMap.get(rowKey)) == null) {
          columnValues = new TreeMap<Cell.Key, ValueCollection>();
          columnCollectionMap.put(rowKey, columnValues);
          saveSize += (rowKeyAllocLen + 80);
        }
      } finally {
        mapLock.writeLock().unlock();
      }
    } 
    
    synchronized(columnValues) {
      ValueCollection valueCollection = columnValues.get(columnValue.getCellKey());
      if(valueCollection == null) {
        valueCollection = new ValueCollection();
        Cell.Key cellKey = columnValue.getCellKey();
        columnValues.put(cellKey, valueCollection);
      
        saveSize += (144 + ((cellKey.getLength() + 3)/8) * 8); //valueCollection 길이
      }
    
      //saveSize += columnValue.size();
      int duplicatedLength = ((rowKeyLength + 3)/8) * 8;
      saveSize += (columnValue.getAllocatedSize() - duplicatedLength);
    
      valueCollection.add(columnValue, numOfVersion);
    }
    
    memorySize += saveSize;
    
    return saveSize;
  }

  public long getMemorySize() {
    return memorySize;
  }
  
  public void print() {
    mapLock.readLock().lock();
    try {
      for(Map.Entry<Row.Key, SortedMap<Cell.Key, ValueCollection>> entry : columnCollectionMap.entrySet()) {
        Row.Key rowKey = entry.getKey();
        SortedMap<Cell.Key, ValueCollection> columnValues = entry.getValue();
      
        System.out.println("\tRow.Key:" + rowKey);
        
        synchronized(columnValues) {
          for(Map.Entry<Cell.Key, ValueCollection> innerEntry: columnValues.entrySet()) {
            System.out.println("\t\tCell.Key:" + innerEntry.getKey());
            innerEntry.getValue().print();
          }
        }
      }
    } finally {
      mapLock.readLock().unlock();
    }
  }

  public ColumnValue get(Row.Key rowKey, Cell.Key columnKey) {
    SortedMap<Cell.Key, ValueCollection> columnValues = null;
    mapLock.readLock().lock();
    try {
      columnValues = columnCollectionMap.get(rowKey);
    } finally {
      mapLock.readLock().unlock();
    }
    
    if(columnValues == null)  return null;
    
    synchronized(columnValues) {
      ValueCollection value = columnValues.get(columnKey);
      if(value == null) return null;
      return value.get();
    }
  }
  
  public ColumnValue[] get(Row.Key rowKey) {
    SortedMap<Cell.Key, ValueCollection> columnValues = null;
    mapLock.readLock().lock();
    try {
      columnValues = columnCollectionMap.get(rowKey);
    } finally {
      mapLock.readLock().unlock();
    }
    
    if(columnValues == null) {
      return null;
    }
    
    List<ColumnValue> ret = new ArrayList<ColumnValue>(10);
    
    synchronized(columnValues) {
      for(ValueCollection value: columnValues.values()) {
        if(!value.isDeleted()) {
          ret.add(value.get());
        }
      }
    }
    
    if(ret.size() == 0) {
      return null;
    }
    
    return ret.toArray(new ColumnValue[]{});
  }
    
  public void print(Row.Key rowKey) {
    SortedMap<Cell.Key, ValueCollection> columnValues = null;
    mapLock.readLock().lock();
    try {
      columnValues = columnCollectionMap.get(rowKey);
    } finally {
      mapLock.readLock().unlock();
    }
    //LOG.debug("get:" + rowKey + ":" + columnValues);
    if(columnValues == null) {
      System.out.println("null");
      return;
    }
    
    synchronized(columnValues) {
      for(ValueCollection value: columnValues.values()) {
        value.print();
      }
    }
  }

  /**
   * 데이터를 임시 디렉토리에 저장한다.
   * @param tabletInfo
   * @param columnName
   * @return
   * @throws IOException
   */
  public TabletMapFile saveToDisk(TabletInfo tabletInfo, 
      String columnName, 
      String fileId,
      int numOfVersion) throws IOException {
    GPath tempMapFilePath = 
      new GPath(Tablet.getTabletMinorCompactionTempPath(conf, tabletInfo), 
          columnName + "/" + fileId + "/");
    
    return saveToDisk(CloudataFileSystem.get(conf),
        tabletInfo, 
        columnName, 
        tempMapFilePath, 
        fileId,
        numOfVersion);
  }

  /**
   * BatchUpload를 위해 임시로 로컬 파일시스템에 저장시킨다.
   * @param fs
   * @param tabletInfo
   * @param columnName
   * @param fileId
   * @return
   * @throws IOException
   */
  public TabletMapFile saveToLocalDisk(CloudataFileSystem fs, 
      TabletInfo tabletInfo, 
      String columnName, 
      String fileId,
      String uploadAcitonId,
      int numOfVersion) throws IOException {
    GPath uploadMapFilePath = new GPath(Tablet.getTabletLocalTempPath(conf, tabletInfo, uploadAcitonId), columnName + "/" + fileId + "/");
    return saveToDisk(fs, tabletInfo, columnName, uploadMapFilePath, fileId, numOfVersion);
  }
  
  private TabletMapFile saveToDisk(CloudataFileSystem fs, TabletInfo tabletInfo, 
      String columnName, GPath parentPath, String fileId, int numOfVersion) throws IOException {
    
    mapLock.readLock().lock();
    try {
      if(columnCollectionMap.isEmpty()) {
        return null;
      }

      //LOG.debug(tabletInfo.getTabletName() + " saveToDisk.columnCollectionMap.size():" + columnCollectionMap.size());
      TabletMapFile tabletMapFile = new TabletMapFile(conf, fs, tabletInfo, columnName, fileId, parentPath, numOfVersion);
      MapFileWriter mapFileWriter = tabletMapFile.getMapFileWriter();

      try {
        for(Map.Entry<Row.Key, SortedMap<Cell.Key, ValueCollection>> entry: columnCollectionMap.entrySet()) {
          for(Map.Entry<Cell.Key, ValueCollection> eachEntry: entry.getValue().entrySet()) {
            mapFileWriter.write(eachEntry.getValue());
          }
        }
      } finally {
        mapFileWriter.close();
      }
      return tabletMapFile;

    } finally {
      mapLock.readLock().unlock();
    }
  }

  public void split(TabletInfo tabletInfo, Row.Key midRowKey, 
      Tablet[] splitedTablets, 
      String columnName, 
      int numOfVersion) throws IOException {
    int tabletIndex = 0;
    mapLock.readLock().lock();
    try {
      //LOG.debug(tabletInfo.getTabletName() + ", before:" + columnCollectionMap.size());
      for(Map.Entry<Row.Key, SortedMap<Cell.Key, ValueCollection>> entry: columnCollectionMap.entrySet()) {
        Row.Key rowKey = entry.getKey();
        
        if(tabletIndex < 1 && rowKey.compareTo(midRowKey) > 0) {
          tabletIndex++;
        }
        SortedMap<Cell.Key, ValueCollection> columnValues = entry.getValue();
        
        ColumnCollection columnCollection = splitedTablets[tabletIndex].memorySSTable.columnCollections.get(columnName);
        if(columnCollection == null) {
          columnCollection = new ColumnCollection(conf);
          splitedTablets[tabletIndex].memorySSTable.columnCollections.put(columnName, columnCollection);
        }
        columnCollection.columnCollectionMap.put(rowKey, columnValues);
      }
    } finally {
      ColumnCollection cols1 = splitedTablets[0].getMemorySSTable().columnCollections.get(columnName);
      int size1 = cols1 == null ? 0 : cols1.columnCollectionMap.size(); 
      ColumnCollection cols2 = splitedTablets[1].getMemorySSTable().columnCollections.get(columnName);
      int size2 = cols2 == null ? 0 : cols2.columnCollectionMap.size();
      
      //LOG.debug(tabletInfo.getTabletName() + ", after:" + size1 + "," + size2);
      mapLock.readLock().unlock();
    }
  }
    
  public TabletMapFile[] splitAndSaveToDisk( TabletInfo tabletInfo, Row.Key midRowKey, 
                                              TabletInfo[] splitedTabletInfos, 
                                              String columnName, 
                                              String fileId,
                                              int numOfVersion) throws IOException {
    TabletMapFile[] tabletMapFiles = new TabletMapFile[2];
    MapFileWriter[] mapFileWriters = new MapFileWriter[2]; 

    for(int i = 0; i < 2; i++) {
      GPath tempMapFilePath = new GPath(Tablet.getTabletSplitTempPath(conf, splitedTabletInfos[i]), columnName + "/" + fileId + "/");
      tabletMapFiles[i] = new TabletMapFile(conf, 
          CloudataFileSystem.get(conf),
          splitedTabletInfos[i], columnName, fileId, tempMapFilePath,
          numOfVersion);
      mapFileWriters[i] = tabletMapFiles[i].getMapFileWriter();
    }
    
    int index = 0;
    CStopWatch watch = new CStopWatch();
    
    mapLock.readLock().lock();
    try {
      for(Map.Entry<Row.Key, SortedMap<Cell.Key, ValueCollection>> entry: columnCollectionMap.entrySet()) {
        Row.Key rowKey = entry.getKey();
        
        if(index < 1 && rowKey.compareTo(midRowKey) > 0) {
          index++;
        }
        SortedMap<Cell.Key, ValueCollection> columnValues = entry.getValue();
        
        synchronized(columnValues) {
          watch.start("write mapFileWriters in split", 1000);
          for(Map.Entry<Cell.Key, ValueCollection> eachEntry: columnValues.entrySet()) {
            mapFileWriters[index].write(eachEntry.getValue());
          }
          watch.stopAndReportIfExceed(LOG);
        }
      }
    } finally {
      mapLock.readLock().unlock();
      for(int i = 0; i < 2; i++) {
        try {   
          mapFileWriters[i].close();  
        } catch (IOException e) { 
          LOG.error(e.getMessage(), e); 
        }
      }
    }
    return tabletMapFiles;    
  }
  
  /**
   * MEAT, ROOT tablet인 경우만 사용된다.
   * 하나의 rowkey에 컬럼의 데이터는 하나만 저장되고, 삭제되지 않은 데이터중 가장 근접한 값을 반환한다.
   * @param rowKey
   * @return
   */
  public ColumnValue findNearestValue(Row.Key rowKey) {
    mapLock.readLock().lock();
    try {
      SortedMap<Row.Key, SortedMap<Cell.Key, ValueCollection>> tailMap = columnCollectionMap.tailMap(rowKey);

      if(!tailMap.isEmpty()) {
        for(Map.Entry<Row.Key, SortedMap<Cell.Key, ValueCollection>> entry: tailMap.entrySet()) {
          SortedMap<Cell.Key, ValueCollection> columnValues = entry.getValue();
          synchronized(columnValues) {
            for(ValueCollection eachValueCollection: columnValues.values()) {

              if(eachValueCollection.get() != null) {
                return eachValueCollection.get();
              }
            }
          }
        }
        return null;
      } else {
        return null;
      } 
    } finally {
      mapLock.readLock().unlock();
    }
  }
  
  public boolean isEmpty() {
    mapLock.readLock().lock();
    try {
      return columnCollectionMap.isEmpty();
    } finally {
      mapLock.readLock().unlock();
    }
  }

  public int getRowCount() {
    mapLock.readLock().lock();
    try {
      return columnCollectionMap.size();
    } finally {
      mapLock.readLock().unlock();
    }
  }
  
  // TODO by sangchul
  // This algorithm takes O(n) time to find mid row key.
  // It seems to be optimized by calculating mid row key in advance 
  // when putting value 
  public Row.Key getMidRowKey() {
    mapLock.readLock().lock();
    try {
      Set<Row.Key> rowKeySet = columnCollectionMap.keySet();
      int rowKeySetSize = rowKeySet.size();
      Iterator<Row.Key> rowKeyIter = rowKeySet.iterator();
      
      if (rowKeySetSize == 2) {
        return rowKeyIter.next();
      } else {
        int count = 0;
        Row.Key ret = null;
        while(rowKeyIter.hasNext()) {
          if (++count >= rowKeySetSize / 2) {
            ret = rowKeyIter.next();
            break;
          }
          
          rowKeyIter.next();
        }
        
        return ret;
      }
    } finally {
      mapLock.readLock().unlock();
    }
  }

  public ColumnValue[] getAllValues() {
    List<ColumnValue> ret = new ArrayList<ColumnValue>();

    Collection<SortedMap<Cell.Key, ValueCollection>> columnValues = null;
    mapLock.readLock().lock();
    try {
      columnValues = columnCollectionMap.values();
    } finally {
      mapLock.readLock().unlock();
    }
    
    for (SortedMap<Cell.Key, ValueCollection> eachValue : columnValues) {
      synchronized(eachValue) {
        for (ValueCollection eachValueCollection : eachValue.values()) {
          ColumnValue columnValue = eachValueCollection.get();
          if (columnValue != null) {
            ret.add(columnValue);
          }
        }
      }
    }

    if (ret.isEmpty()) {
      return null;
    } else {
      return ret.toArray(new ColumnValue[columnValues.size()]);
    }
  }

  public Collection<? extends Key> getRowKeySet() {
    mapLock.readLock().lock();
    try {
      return columnCollectionMap.keySet();
    } finally {
      mapLock.readLock().unlock();
    }
  }

  public void search(Row.Key rowKey, CellFilter cellFilter, List<ColumnValue> result) {
    SortedMap<Cell.Key, ValueCollection> rowDatas = null;
    mapLock.readLock().lock();
    try {
      rowDatas = columnCollectionMap.get(rowKey);
    } finally {
      mapLock.readLock().unlock();
    }
    
    if (rowDatas != null) {
      synchronized(rowDatas) {
        for (ValueCollection vc : rowDatas.values()) {
          // RPC 속도에 최적화하기 위해 RPC 전송전 columnValue의 값이 변경될 수 있는데
          // 동일한 레퍼런스를 사용할 경우 MemorySSTable의 값도 변경되기 때문에 복사해서 사용
          for (ColumnValue eachColumnValue : vc.getColumnValues()) {
            if(cellFilter.matchKey(eachColumnValue.getCellKey())) {
              result.add(eachColumnValue.copyColumnValue());
            }
          }
        }
      }
    }
  }

  public void searchAllVersion(Key rowKey, Cell.Key cellKey, List<ColumnValue> result) {
    SortedMap<Cell.Key, ValueCollection> columnValues = null;
    mapLock.readLock().lock();
    try {
      columnValues = columnCollectionMap.get(rowKey);
    } finally {
      mapLock.readLock().unlock();
    }
    
    if (columnValues != null) {
      synchronized(columnValues) {
        ValueCollection vc = columnValues.get(cellKey);
        if (vc != null) {
          for (ColumnValue eachColumnValue : vc.getColumnValues()) {
            result.add(eachColumnValue.copyColumnValue());
          }
        }
      }
    }
  }
  
  public void searchAllVersion(Key rowKey, Collection<ColumnValue> result) {
    SortedMap<Cell.Key, ValueCollection> columnValues = null;
    mapLock.readLock().lock();
    try {
      columnValues = columnCollectionMap.get(rowKey);
    } finally {
      mapLock.readLock().unlock();
    }
    
    if (columnValues != null) {
      synchronized(columnValues) {
        for(ValueCollection eachVC: columnValues.values()) {
          result.addAll(eachVC.getColumnValues());
        }
      }
    }
  }
  
  public static void main(String[] args) throws Exception {
    /*
    Assuming 32-bit words and a 12 byte overhead per array instance,
    int[4,100,000][2][2][5]
    4th dim. = overhead + length * sizeof(int) = 12 + 5 * 4 = 32
    3rd dim. = overhead + length * (sizeof(ref) + sizeof(4th_dim)) = 12 + 2 * (4 + 32) = 84
    2nd dim. = overhead + length * (sizeof(ref) + sizeof(3rd_dim)) = 12 + 2 * (4 + 84) = 188
    1st dim. = overhead + length * (sizeof(ref) + sizeof(2nd_dim)) = 12 + 4,100,000 * (4 + 188) = 787,200,012
    
    byte[]의 길이 = 12 + ((length + 3)/8) * 8 + 4
    
    byte의 경우 0 ~ 4까지는 12 + 4가 되지만
     그 이상의 경우 8씩 증가
     예: 0 ~ 4: 16, 5 ~ 12: 24, 13 ~ 20: 32 
     
     따라서 Text의 경우 객체 header 4byte, byte[] 레퍼런스 4byte, int length 4byte, length의 레퍼런스 4 byte = 16 + len(byte[])
    */    

    //Cell.Key, Row.Key: 16byte + byte[]
    //ColumnValue: 16 + rowKey, columnKey, value
    //ColumnCollection: ColumnValue + 124
    
    Runtime runtime = Runtime.getRuntime();
    Cell.Key columnKey = new Cell.Key("123");
    Row.Key rowKey = new Row.Key("123");
    ColumnValue columnValue = new ColumnValue();
        
    ColumnCollection collection = new ColumnCollection(new CloudataConf());
    collection.addValue(rowKey, new ColumnValue(rowKey, columnKey, "".getBytes()), 0);
    
    byte[] valueBuf = "0123456789".getBytes();
    byte[] columnKeyBuf = "abcdefghij".getBytes();
    byte[] rowKeyBuf = "!@#$%^&*()".getBytes();

    Cell.Key columnKey2 = new Cell.Key("qwertyuiop".getBytes());
    Row.Key rowKey2 = new Row.Key("asdfghjkl;".getBytes());
    ColumnValue columnValue2 = new ColumnValue(rowKey2, columnKey2, "zxcvbnm,./".getBytes());

    columnKey = new Cell.Key(columnKeyBuf);
    rowKey = new Row.Key(rowKeyBuf);
    columnValue = new ColumnValue(rowKey, columnKey, valueBuf);
    
    runtime.gc();
    long before = runtime.freeMemory();

    collection.addValue(rowKey, columnValue, 0);
    collection.addValue(rowKey2, columnValue2, 0);
    
    runtime.gc();
    long after = runtime.freeMemory();
    
    int byteLen = 12 + ((valueBuf.length + 3) / 8) * 8 + 4;
    int rowKeyLen = 16 + byteLen;
    int columnKeyLen = 16 + byteLen;
    
    int columnValueLen = 16 + rowKeyLen + columnKeyLen + byteLen;
    
    System.out.printf("%d - %d = %d, %d\n", before, after, (before-after), columnValueLen);
  }
}
