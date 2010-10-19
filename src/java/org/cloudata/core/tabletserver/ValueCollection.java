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

import java.io.DataOutputStream;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CellFilter;
import org.cloudata.core.client.CellValueMatcher;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.RowFilter;
import org.cloudata.core.common.exception.GetFailException;
import org.cloudata.core.tablet.ColumnValue;


/**
 * Cell.Key에 대응하는 값을 가지고 있다.
 * 하나의 Cell.Key에는 여러 버전의 value가 존재하게 된다.
 * 각각의 value는 timestamp를 가지고 있으며 timestamp로 desc 정렬되어 저장된다.
 * 일반적인 조회 요청에는 가장 최근의 데이터를 반환하게 된다.
 * @author babokim
 *
 */
public class ValueCollection implements Iterator<ColumnValue>{
  public static final Log LOG = LogFactory.getLog(ValueCollection.class.getName());
  
//  protected Cell.Key columnKey;
  
  protected SortedSet<ColumnValue> columnValues = new TreeSet<ColumnValue>();
  
  protected Iterator<ColumnValue> iterator; 
  
  public ValueCollection() {
    
  }
  
//  protected ValueCollection(Cell.Key columnKey) {
//    this.columnKey = columnKey;
//  }
  
  public void clear() {
    columnValues.clear();
    iterator = null;
  }
  
  public void setColumnRecords(SortedSet<ColumnValue> columnValues) {
    this.columnValues = columnValues;
    iterator = columnValues.iterator();
  }
  
  public void add(ColumnValue columnValue, int numOfVersion) {
    columnValues.add(columnValue);
    fitToNumOfVersion(numOfVersion);
  }

  private void fitToNumOfVersion(int numOfVersion) {
    //0이면 무제한으로 저장
    if(numOfVersion == 0) {
      return;
    }
    
    int size = columnValues.size();
    if(size > numOfVersion) {
      for(int i = 0; i < (size - numOfVersion); i++) {
        columnValues.remove(columnValues.last());
      }
    }    
  }
//  public String getValueAsString1() {
//    if(columnValues.size() > 0) {
//      return columnValues.first().getValueAsString();
//    } else {
//      return "";
//    }
//  }

  protected ColumnValue getWithDeleted() {
    return columnValues.isEmpty() ? null : columnValues.first();  
  }
  
  protected ColumnValue[] getWithDeleted(RowFilter scanFilter) {
    if(columnValues.isEmpty()) {
      return null;
    }
    //FIXME filter 적용되도록(여기서 하는 것이 맞는지, Searcher에서 하는 것이 맞는지????
    ColumnValue columnValue = getWithDeleted();
    if(columnValue == null) {
      return null;
    } else {
      return new ColumnValue[]{columnValue};
    }

//    if(scanFilter.isEmpty()) {
//      ColumnValue columnValue = getWithDeleted();
//      if(columnValue == null) {
//        return null;
//      } else {
//        return new ColumnValue[]{columnValue};
//      }
//    }
//    
//    List<ColumnValue> result = new ArrayList<ColumnValue>();
//    for(ColumnValue eachColumnResult: columnValues) {
//      if(eachColumnResult.match(scanFilter)) {
//        result.add(eachColumnResult);
//      }
//    }
//    
//    if(result.size() == 0) {
//      return null;
//    } else {
//      return result.toArray(new ColumnValue[result.size()]);
//    }
  }
  
  public ColumnValue get() {
    if(!columnValues.isEmpty()) {
      ColumnValue columnValue = columnValues.first();
      if(columnValue.isDeleted())  return null;
      
      return columnValue;
    } else {
      return null;
    }
  }

//  public ColumnValue[] get(ColumnFilter columnFilter) {
//    if(columnValues.size() == 0) {
//      return null;
//    }
//    ColumnValue columnValue = get();
//    if(columnValue == null) {
//      return null;
//    } else {
//      return new ColumnValue[]{columnValue};
//    }
//  }
  
  public ColumnValue[] get(int numVersion) {
    if(columnValues.isEmpty()) {
      return null;
    }
    
    int size = columnValues.size() > numVersion ? numVersion : columnValues.size();
    
    ColumnValue[] result = new ColumnValue[size];
    
    int index = 0;
    for(ColumnValue eachColumnRecord: columnValues) {
      result[index++] = eachColumnRecord;
      if(index >= size) break;
    }
    
    return result;
  }
  
  public ColumnValue getLast() {
    if(!columnValues.isEmpty()) {
      return columnValues.last();
    } else {
      return null;
    }
  }

  public int write(DataOutputStream dataOut) throws IOException {
    int writeBytes = 0;
    //FIXME Map 파일에 저장할 버전 갯수에 따라 저장(갯수, 시간)
    for(ColumnValue eachColumnValue : columnValues){
      MapFileColumnValue mapFileColumnValue = new MapFileColumnValue();
      copyMapFileColumnValue(eachColumnValue, mapFileColumnValue);
      mapFileColumnValue.write(dataOut);
      writeBytes += mapFileColumnValue.size();
    }
    return writeBytes;
  }
  
  //테스트 용도
  public int getByteSize() throws IOException {
    int writeBytes = 0;
    for(ColumnValue eachColumnValue : columnValues){
      MapFileColumnValue mapFileColumnValue = new MapFileColumnValue();
      copyMapFileColumnValue(eachColumnValue, mapFileColumnValue);
      writeBytes += mapFileColumnValue.size();
    }
    return writeBytes;
  }
  
  public static void copyMapFileColumnValue(ColumnValue columnValue, MapFileColumnValue mapFileColumnValue) {
    mapFileColumnValue.setRowKey(columnValue.getRowKey());
    mapFileColumnValue.setCellKey(columnValue.getCellKey());
    mapFileColumnValue.setValue(columnValue.getValue());
    mapFileColumnValue.setTimestamp(columnValue.getTimestamp());
    mapFileColumnValue.setDeleted(columnValue.isDeleted());
  }
  
  public void addAll(ValueCollection collection, int numOfVersion) {
    if(collection == null)  return;
    columnValues.addAll(collection.getColumnValues());
    fitToNumOfVersion(numOfVersion);
  }

  public SortedSet<ColumnValue> getColumnValues() {
    return columnValues;
  }
  
  public boolean isEmpty() {
    return columnValues.isEmpty();
  }

  public int getValueSize() {
    return columnValues.size();
  }

  public void print() {
    System.out.println("\t\t\t-------------------------");
    for(ColumnValue record : columnValues){
      System.out.println("\t\t\t" + record);
    }
    System.out.println("\t\t\t-------------------------");
  }

//  public ValueCollection copy(long timestamp) {
//    ValueCollection copyedValue = new ValueCollection();
//    if(columnValues.size() == 0) {
//      return copyedValue;
//    }
//    ColumnValue firstRecord = columnValues.first();
//    ColumnValue filterColumnRecord = new ColumnValue(firstRecord.getRowKey(), firstRecord.getCell.Key(), null, timestamp);
//
//    SortedSet<ColumnValue> copyedColumnRecords = columnValues.tailSet(filterColumnRecord);
//    
//    copyedValue.setColumnRecords(copyedColumnRecords);
//    
//    return copyedValue;
//  }

  class CellFilterMatcher {
    CellFilter filter;
    boolean saveValueSize;
    int columnValueSize;
    CellValueMatcher valueMatcher;
    
    CellFilterMatcher(CellFilter filter, boolean saveValueSize) throws IOException {
      this.filter = filter;
      this.saveValueSize = saveValueSize;
      if (saveValueSize) {
        this.columnValueSize = columnValues.size();
      }
      
      valueMatcher = filter.getCellValueMatcher();
    }
    
    void saveMatchedResultsTo(List<ColumnValue> result) throws IOException {
      if (filter.getNumOfVersions() > 1) {
        processMultipleVersions(result);
      } else if(filter.hasTimestampFilter()) {
        processTimeStampFilter(result);
      } else {
        processDefaultCellFilter(result);
      }
    }
    
    void processTimeStampFilter(List<ColumnValue> result) {
      long startTime = filter.getStartTimestamp();
      long endTime = filter.getEndTimestamp();

      for(ColumnValue columnValue: columnValues) {
        long valueTimestamp = columnValue.getTimestamp();
        if(valueTimestamp >= startTime && valueTimestamp <= endTime ) {
          if(!checkPaging(columnValue)) {
            continue;
          }
          if (saveValueSize) {
            columnValue.setNumOfValues(columnValueSize);
          }
          result.add(columnValue);
        }
      }
    }

    void processMultipleVersions(List<ColumnValue> result) throws IOException {
      int versionCount = 0;
      int numOfVersion = filter.getNumOfVersions();
      
      for(ColumnValue columnValue: columnValues) {
        if(!checkPaging(columnValue)) {
          continue;
        }
        if(valueMatcher != null) {
          try {
            if(!valueMatcher.match(columnValue.getValue(), columnValue.getTimestamp(), filter.getFilterParams())) {
              continue;
            }
          } catch (Exception e) {
            throw new GetFailException(e);
          }
        } 
        if (saveValueSize) {
          columnValue.setNumOfValues(columnValueSize);
        }
        result.add(columnValue);
        versionCount++;
        if(versionCount >= numOfVersion) {
          break;
        }
      }
    }

    void processDefaultCellFilter(List<ColumnValue> result) throws IOException {
      ColumnValue columnValue = get();
      if(columnValue != null) {
        try {
          if(valueMatcher != null && 
              !valueMatcher.match(columnValue.getValue(), columnValue.getTimestamp(), filter.getFilterParams())) {
            return;
          }
        } catch (Exception e) {
          throw new GetFailException(e);
        }
        if(!checkPaging(columnValue)) {
          return;
        }
        if (saveValueSize) {
          columnValue.setNumOfValues(columnValueSize);
        }
        result.add(columnValue);
      }
    }
    
    boolean checkPaging(ColumnValue columnValue) {
      //targetCellKey와 같은 경우 pagingTimestamp와도 비교해야 함
      Cell.Key targetCellKey = filter.getTargetCellKey();
      if(targetCellKey != null && columnValue.getCellKey().equals(targetCellKey)) {
        long pagingTimestamp = filter.getPagingTimestamp();
        if(columnValue.getTimestamp() < pagingTimestamp) {
          return false;
        }
      }
      
      return true;
    }
  }
  
  public void moveMatchedValues(CellFilter cellFilter, List<ColumnValue> result) throws IOException {
    if(columnValues.isEmpty()) {
      return;
    }
    
    new CellFilterMatcher(cellFilter, true).saveMatchedResultsTo(result);
  }

  public List<ColumnValue> get(CellFilter cellFilter) throws IOException {
    //columnFilter 중 시간, 버전에 대한 처리는 여기서 한다.
    if(columnValues.isEmpty()) {
      return null;
    }

    List<ColumnValue> result = new ArrayList<ColumnValue>();
    
    new CellFilterMatcher(cellFilter, false).saveMatchedResultsTo(result);
    
    return result;
  }

  public boolean isDeleted() {
    if(columnValues.size() > 0) {
      return columnValues.first().isDeleted();
    } else {
      return true;
    }
  }

  public boolean hasNext() {
    if(iterator == null)  return false;
    return iterator.hasNext();
  }

  public ColumnValue next() {
    return iterator.next();
  }

  public void remove() {
  }
  
  public static void main(String[] args) throws IOException {
    //속도 테스트
    TreeMap<Cell.Key, ValueCollection> values = new TreeMap<Cell.Key, ValueCollection>();

    int size = 10485;
//    int size = 100;
    ColumnValue[] columnValues = new ColumnValue[size];
    DecimalFormat df = new DecimalFormat("0000000000");
    String value = "012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";
    Row.Key rowKey = new Row.Key(df.format(100));
    
    byte[] valueBytes = value.getBytes();
    for(int i = 0; i < size; i++) {
      columnValues[i] = new ColumnValue();
      columnValues[i].setRowKey(rowKey);
      columnValues[i].setCellKey(new Cell.Key(df.format(i)));
      columnValues[i].setValue(valueBytes);
    }

    long startTime = System.currentTimeMillis();
    for(int i = 0; i < size; i++) {
      ValueCollection valueCollection = new ValueCollection();
//      SortedSet<ColumnValue> set = new TreeSet<ColumnValue>();
//      set.add(columnValues[i]);
      valueCollection.add(columnValues[i], 1);
    }
    long endTime = System.currentTimeMillis();
//    System.out.println(">>>>" + (endTime - startTime));
    
    startTime = System.currentTimeMillis();
    long tatalNano = 0;
    for(int i = 0; i < size; i++) {
      long nano1 = System.nanoTime();
      ValueCollection valueCollection = values.get(columnValues[i].getCellKey());
      long nano2 = System.nanoTime();
      if(valueCollection == null) {
        valueCollection = new ValueCollection();
        values.put(columnValues[i].getCellKey(), valueCollection);
      }
      long nano3 = System.nanoTime();
      valueCollection.add(columnValues[i], 1);
      long nano4 = System.nanoTime();
      tatalNano += (nano3 - nano2);
//      System.out.println((nano4-nano3) + "," + (nano3-nano2) + "," + (nano2-nano1));
    }
    endTime = System.currentTimeMillis();
    
    //System.out.println(">>>>" + (endTime - startTime) + ">" + (tatalNano/size));
  }
}
