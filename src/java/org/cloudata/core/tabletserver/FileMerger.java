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
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.DefaultPerformanceRegulator;
import org.cloudata.core.common.CStopWatch;
import org.cloudata.core.common.NullPerformanceRegulator;
import org.cloudata.core.common.PerformanceRegulator;
import org.cloudata.core.tablet.ColumnValue;
import org.cloudata.core.tabletserver.TabletMapFile.MapFileReader;
import org.cloudata.core.tabletserver.TabletMapFile.MapFileWriter;
import org.cloudata.core.tabletserver.action.MajorCompactionAction;


/**
 * 
 * @author babokim
 * MajorCompaction시 여러개의 MapFile을 merge하여 하나의 파일로 만든다.
 *
 */
public class FileMerger {
  public static final Log LOG = LogFactory.getLog(FileMerger.class.getName());
  
  public void merge(TabletMapFile resultFile, TabletMapFile[] targetTabletMapFiles, boolean debug, int numOfVersion) throws IOException {
    //FIXME 객체를 매번 생성하지 말고 한번 생성한 객체를 재사용하도록 변경
    MapFileWriter writer = resultFile.getMapFileWriter();
    MapFileReader[] readers = null;
    long totalMapFileSize = 0;
    try {
      readers = new MapFileReader[targetTabletMapFiles.length];
      for (int i = 0; i < targetTabletMapFiles.length; i++) {
        if(targetTabletMapFiles[i] != null) {
          totalMapFileSize += targetTabletMapFiles[i].getDataFileSize();
          readers[i] = targetTabletMapFiles[i].getMapFileReader(Row.Key.MIN_KEY, Row.Key.MAX_KEY);
        }
      }

      //Compaction 도중에 kill된 경우 중복된 데이터가 다른 map file에 존재할 수 있다.
      //따라서 동일한 레코드에 대한 처리를 고려해야 한다.
      List<RecordItem> worker = new ArrayList<RecordItem>();
      
      // init
      initWorker(worker, readers);
      
//      if (!MajorCompactionAction.isFastProcessingMode()) {
//        try {
//          Thread.sleep(100);
//        } catch(InterruptedException e) {
//        }
//      }

      boolean isCompleted = false;
      
      RecordItem oldWinnerItem = null;
      ValueCollection values = new ValueCollection();
      
      int recordCount = 0; 

      List<Integer> removedIndexs = new ArrayList<Integer>(10);

      PerformanceRegulator regulator = createRegulator(totalMapFileSize);
      
      while (!isCompleted) {
        if (worker.isEmpty()) {
          if (!initWorker(worker, readers)) {
            writer.write(values);            
            break;
          }
        }
        
//        if (worker.size() > 100) {
//          LOG.warn("size of worker exceeds 100");
//        }
       
        Collections.sort(worker);
        
        RecordItem winnerItem = worker.remove(0);
        int winnerIndex = winnerItem.getIndex();

        if(oldWinnerItem == null) {
          oldWinnerItem = winnerItem;
        }
        ColumnValue winnerColumnValue = winnerItem.columnValue;
        
        //worker에 동일한 ColumnValue가 존재하는지 확인한다.
        //동일한 ColumnValue가 존재하는 경우 해당 ColumnValue를 가지고 있는 reader에서 다음 레코드를 읽어오도록 한다.
        int index = 0;
        for(RecordItem eachItem: worker) {
          if(eachItem.columnValue.equals(winnerColumnValue)) {
            removedIndexs.add(index);
          }
          index++;
        }
        
        index = 0;
        for(int removeIndex: removedIndexs) {
          RecordItem removedItem = worker.remove(removeIndex - index);
          ColumnValue nextColumn = null;
          if(readers[removedItem.index] != null) {
            nextColumn = readers[removedItem.index].next();
          }
          
          if (nextColumn != null) {
            worker.add(new RecordItem(removedItem.index, readers[removedItem.index].getCurrentRowKey(), nextColumn));
            if(nextColumn.getValue() != null) {
              regulator.recordRead(nextColumn.getValue().length);
            }
          } else {
            try {
              if(readers[removedItem.index] != null) {
                readers[removedItem.index].close();
              }
            } catch (IOException e) {
              //close가 되어 있는 경우 다시 close 하면 exception이 발생한다.
              LOG.info(e.getMessage());
            }
            readers[removedItem.index] = null;
          }
          index++;
        }
        removedIndexs.clear();
        
        //새로운 columnkey에 대한 값이 read된 경우 기존 값을 file에 write한다.
        if(!oldWinnerItem.equalsCellKey(winnerItem)) {
          int nw = writer.write(values);
          regulator.recordWrite(nw);
          
          oldWinnerItem = winnerItem;
          values = new ValueCollection();
        } 

        values.add(winnerColumnValue, numOfVersion);
        
        // remove first key
        winnerColumnValue = null;
        winnerItem = null;
        
        // read one line from winner file and add
        ColumnValue nextColumn = readers[winnerIndex].next();
        if (nextColumn != null) {
          worker.add(new RecordItem(winnerIndex, readers[winnerIndex].getCurrentRowKey(), nextColumn));
          //regulator.recordRead(nextColumn.getValue().length);
        } else {
          try {
            readers[winnerIndex].close();
          } catch (Exception e) {
            LOG.info(e.getMessage());
          }
          readers[winnerIndex] = null;
        }
        recordCount++;
      } //end of while
    } catch (Exception e) {
      LOG.error("merge error:" + e.getMessage(), e);
      IOException err = new IOException(e.getMessage());
      err.initCause(e);
      throw err;
    } finally {
      if (readers != null) {
        for (int i = 0; i < readers.length; i++) {
          if (readers[i] != null) {
            try {
              readers[i].close();
            } catch (Exception e) {
              LOG.info(e.getMessage());
            }
          } 
        }
      }
      try {
        if(writer != null) {
          writer.close();
        }
        
//        LOG.info(resultFile.getTabletInfo().getTabletName() + " file merged from orig: " + totalMapFileSize 
//            + " to " + resultFile.getDataFileSize());
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
        throw e;
      }
    }
  }

  private PerformanceRegulator createRegulator(long totalMapFileSize) {
    return new NullPerformanceRegulator();
  }

  private boolean initWorker(Collection<RecordItem> worker, MapFileReader[] readers) throws IOException {
    int readersLength = readers.length;

    boolean nodata = true;
    for (int i = 0; i < readersLength; i++) {
      if (readers[i] != null) {
        nodata = false;
        break;
      }
    }
    if (nodata)
      return false;

    nodata = true;
    for (int i = 0; i < readersLength; i++) {
      if(readers[i] == null) {
        continue;
      }
      try {
        ColumnValue columnValue = readers[i].next();
        if (columnValue != null) {
          RecordItem item = new RecordItem(i, readers[i].getCurrentRowKey(), columnValue);
          worker.add(item);
          nodata = false;
        } else {
          try {
            readers[i].close();
          } catch (Exception e) {
            LOG.info(e.getMessage());
          }
          readers[i] = null;
        }
      } catch (Exception e) {
        throw new IOException(e.getMessage());
      }
    }
    
    return !nodata;
  }
  
  static class RecordItem implements Comparable<RecordItem> {
    int index;
    Row.Key rowKey;
    ColumnValue columnValue;
    public RecordItem(int index, Row.Key rowKey, ColumnValue columnValue) {
      this.index = index;
      this.rowKey = rowKey;
      this.columnValue = columnValue;
    }
    public boolean equalsCellKey(RecordItem winnerItem) {
      if(!rowKey.equals(winnerItem.getRowKey())) {
        return false;
      }
      return columnValue.getCellKey().equals(winnerItem.columnValue.getCellKey());
    }
    public int getIndex() {
      return index;
    }
    public Row.Key getRowKey() {
      return rowKey;
    }
    
    public String toString() {
      return rowKey.toString() + "," + columnValue.toString();
    }
    public int compareTo(RecordItem item) {
      int rowKeyCompare = rowKey.compareTo(item.rowKey);
      return rowKeyCompare != 0 ? rowKeyCompare : columnValue.compareTo(item.columnValue);
    }
  }
}
