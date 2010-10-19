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
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.client.CellFilter;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.tablet.ColumnValue;
import org.cloudata.core.tablet.TabletInfo;


public class TabletScanner {
  public static final Log LOG = LogFactory.getLog(TabletScanner.class.getName());
  
  public static Random rand = new Random();
  
  private String scannerId;
  
  private TreeSet<ColumnRecordItem> columnValueBuf = new TreeSet<ColumnRecordItem>(); 
  
  private Scanner[] scanners;

  private int fetchSize;
  
  private CellFilter cellFilter;
  
  private int numOfVersion;

  public TabletScanner(String scannerId,
                        CloudataConf conf,
                        TabletInfo tabletInfo, 
                        Row.Key startRowKey, 
                        Row.Key endRowKey, 
                        String columnName,
                        MemorySSTable memorySSTable,
                        DiskSSTable diskSSTable, 
                        CellFilter cellFilter,
                        int numOfVersion,
                        boolean clientSide) throws IOException {
    
    //LOG.debug(tabletInfo.getTabletName() + "[" + startRowKey + "][" + endRowKey + "[" + columnName + "][" + numOfVersion + "][" + clientSide + "]");
    //LOG.debug("[" + cellFilter.toString() + "]");
    
    this.cellFilter = cellFilter;
    
    this.fetchSize = conf.getInt("scanner.fetch.size", 50);
    
    if(cellFilter.hasTimestampFilter()) {
      this.numOfVersion = 0;
    } else {
      this.numOfVersion = numOfVersion;
    }
    
    this.scannerId = scannerId;
    
    Scanner[] mapFileScanners = null;
    if(diskSSTable != null) {
      mapFileScanners = diskSSTable.getMapFileScanners(columnName, startRowKey, endRowKey, cellFilter);
    }
    int mapFileScannerCount = (mapFileScanners == null) ? 0 : mapFileScanners.length;
    
    scanners = new Scanner[mapFileScannerCount + 1];
    scanners[0] = memorySSTable.getScanner(columnName, startRowKey, endRowKey, cellFilter, clientSide);
    if(mapFileScannerCount > 0) {
      System.arraycopy(mapFileScanners, 0, scanners, 1, mapFileScannerCount);
    }
    
    for (int i = 0; i < scanners.length; i++) {
      ColumnValue columnValue = scanners[i].next();
      
      if(columnValue != null) {
        columnValueBuf.add(new ColumnRecordItem(i, columnValue));
      } else {
        scanners[i].close();
        scanners[i] = null;
      }
    }
  }

  /**
   * 한번에 여러개의 결과를 반환한다.
   * 이것은 클라이언트 모듈에서 cache로 저장되어 서버와의 접속 횟수를 줄여준다.
   * @return
   * @throws IOException
   */
  public ColumnValue[] next() throws IOException {
    List<ColumnValue> results = new ArrayList<ColumnValue>(fetchSize);

    ValueCollection valueCollection = new ValueCollection();
    while(true) {
      if(columnValueBuf.isEmpty())  break;
      
      ColumnRecordItem firstResultItem = columnValueBuf.first();
      columnValueBuf.remove(firstResultItem);
      ColumnValue firstResult = firstResultItem.getColumnRecord();
        
      int winnerScannerIndex = firstResultItem.getIndex();

      valueCollection.add(firstResult, numOfVersion);
      
      if(scanners[winnerScannerIndex] != null) {
        ColumnValue nextRecord = scanners[winnerScannerIndex].next();
        if(nextRecord != null) {
          columnValueBuf.add(new ColumnRecordItem(winnerScannerIndex, nextRecord));
        } else {
          scanners[winnerScannerIndex].close();
          scanners[winnerScannerIndex] = null;
        }
      }
      
      List<ColumnRecordItem> items = new ArrayList<ColumnRecordItem>();
      while(true) {
        boolean hasSameKey = false;
        //현재의 버퍼에 동일한 rowkey, columnKey를 가지고 있는 다른 버전을 찾는다.
        items.addAll(columnValueBuf);
        for(ColumnRecordItem eachItem: items) {
          ColumnValue eachColumnRecord = eachItem.getColumnRecord();
          if(firstResult.equalsKey(eachColumnRecord)) {
            columnValueBuf.remove(eachItem);
            
            valueCollection.add(eachColumnRecord, numOfVersion);
            
            //valueCollection.print();
            if(scanners[eachItem.index] != null) {
              ColumnValue nextRecord = scanners[eachItem.index].next();
              if(nextRecord != null) {
                columnValueBuf.add(new ColumnRecordItem(eachItem.index, nextRecord));
              } else {
                scanners[eachItem.index].close();
                scanners[eachItem.index] = null;
              }
            }
            hasSameKey = true;
          }
        }
        items.clear();
        if(!hasSameKey || columnValueBuf.isEmpty()) {
          break;
        }
      }
      items = null;
      //valueCollection.print();

      //여러개의 scanner로부터 가져온 데이터를 이용하여 filter조건에 맞는 값을 가져온다.
      if(valueCollection.isEmpty()) {
        break;
      } else {
        List<ColumnValue> scanedRecords = valueCollection.get(cellFilter);
        if(scanedRecords != null && scanedRecords.size() > 0) {
          int numOfValue = valueCollection.getValueSize();
          //FIXME 여기서 ColumnValue에 대해 copy를 해서 사용해야 할 것 같다.
          //Scanner가 동시에 여러개 open되면 NumOfValues 값에 동기화 문제가 발생함....
          for(ColumnValue eachColumnValue: scanedRecords) {
            eachColumnValue.setNumOfValues(numOfValue);
            //LOG.info("results.add:" + scanedRecords[i]);
            results.add(eachColumnValue);
          }
        }
      }
      valueCollection.clear();
      if(results.size() >= fetchSize) {
        break;
      }
    }
    valueCollection = null;
    
    if(results.isEmpty()) {
      return null;
    }
    return results.toArray(new ColumnValue[]{});
  }

  public String getScannerId() {
    return scannerId;
  }
  
  public void close() throws IOException {
    for(int i = 0; i < scanners.length; i++) {
      if(scanners[i] != null) {
        scanners[i].close();
      }
    }
  }

  static class ColumnRecordItem implements Comparable<ColumnRecordItem> {
    int index;
    ColumnValue columnValue;
    
    public ColumnRecordItem(int index, ColumnValue columnValue) {
      this.index = index;
      this.columnValue = columnValue;
    }


    public int getIndex() {
      return index;
    }

    public ColumnValue getColumnRecord() {
      return columnValue;
    }

    public int compareTo(ColumnRecordItem rowColumnValuesItem) {
      return columnValue.compareTo(rowColumnValuesItem.columnValue);
    }
    
    public boolean equals(Object obj) {
      if( !(obj instanceof ColumnRecordItem) ) {
        return false;
      }
      
      return compareTo((ColumnRecordItem)obj) == 0;
    }    
  }

  public static String generateScannerId(TabletInfo tabletInfo) {
    return tabletInfo.getTabletName() + ":" + String.valueOf(System.nanoTime()) + String.valueOf(rand.nextInt());
  }
}