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
package org.cloudata.core.client.blob;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.ZooKeeper;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CellFilter;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.RowFilter;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.lock.LockUtil;
import org.cloudata.core.fs.CloudataFileSystem;
import org.cloudata.core.fs.GPath;
import org.cloudata.core.tablet.TableSchema;
import org.cloudata.core.tablet.TableSchema.ColumnInfo;


/**
 * @author jindolk
 *
 */
public class BlobManager extends Thread  {
  private static final Log LOG = LogFactory.getLog(BlobManager.class.getName());
  
  private static final int NUM_THREAD_POOL = 5;
  
  private boolean stop = false;

  private CloudataConf conf;
  
  private long maxFileLength;
  
  private float gcRate;
  
  private static final int ROW_FETCH_SIZE = 10;
  
  private static final int NUM_CELL_PER_FILE = 1000; 
//  private static final int NUM_CELL_PER_FILE = 100;
  
  private ThreadPoolExecutor mergerThreadExecutor = (ThreadPoolExecutor)Executors.newFixedThreadPool(NUM_THREAD_POOL);
  
  private Set<Row.Key> pendingMergeFiles = new HashSet<Row.Key>(); 
  
  private long numTotalMergedFiles;
  
  private CloudataFileSystem fs;
  
  private ZooKeeper zk;
  
  public BlobManager(CloudataConf conf) throws Exception {
    this.conf = conf;
    this.conf.set("client.max.resultRecord",  ROW_FETCH_SIZE * NUM_CELL_PER_FILE);
    this.maxFileLength = conf.getLong("blob.maxFileLength", 64) * 1024 * 1024;   //1GB
    this.gcRate = conf.getFloat("blob.gcRate", 0.2f) ;
    
    this.fs = CloudataFileSystem.get(conf); 
    this.zk = LockUtil.getZooKeeper(conf, "BlobManager", null);
  }
  
  public long getNumTotalMergedFiles() {
    return numTotalMergedFiles;
  }
  
  public void run() {
    startBlobManager();
  }
  
  public void setMaxFileLength(long maxFileLength) {
    this.maxFileLength = maxFileLength;
  }
  
  public void shutdown() {
    stop = true;
    mergerThreadExecutor.shutdownNow();
    this.interrupt();
    System.exit(0);
  }
  
  public void error(Exception e) {
    shutdown();
  }
  
  public void addPendingMergeFile(String path) {
    synchronized(pendingMergeFiles) {
      pendingMergeFiles.add(new Row.Key(path));
    }
  }
  
  public void endMerge(List<String> paths) {
    synchronized(pendingMergeFiles) {
      for(String path: paths) {
        numTotalMergedFiles++;
        pendingMergeFiles.remove(new Row.Key(path));
      }
    }
  }
  
  private void startBlobManager()  {
    LOG.info("Blob Manager started");
    while(!stop) {
      waitMergerThreadPool();
      
      try {
        //find all blob columns
        TableSchema[] tableSchemas = CTable.listTables(conf);
        for(TableSchema eachSchema: tableSchemas) {
          for(ColumnInfo eachColumn: eachSchema.getColumnInfos()) {
            if(eachColumn.getColumnType() == TableSchema.BLOB_TYPE) {
              //LOG.debug("found merge target:" + eachSchema.getTableName() + "," + eachColumn.getColumnName());
              mergeBlobFiles(eachSchema.getTableName(), eachColumn.getColumnName());
            }
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
        LOG.error(e.getMessage(), e);
      }

      try {
        Thread.sleep(10 * 1000);
      } catch (InterruptedException e) {
        return;
      }
    }
    LOG.info("Blob Manager stopped");
  }

  private void waitMergerThreadPool() {
    boolean first = true;
    while(!stop) {
      if(mergerThreadExecutor.getActiveCount() < mergerThreadExecutor.getMaximumPoolSize() / 2) {
        return;
      }
      if(first) {
        LOG.info("Waiting thread pool capacity:" + mergerThreadExecutor.getActiveCount());
        first = false;
      }
      try {
        Thread.sleep(5 * 1000);
      } catch (InterruptedException e) {
        return;
      }
    }
  }

  private void mergeBlobFiles(String tableName, String columnName) throws IOException {
    String blobTableName = BlobMetaManager.getBlobTableName(tableName, columnName);
    if(!CTable.existsTable(conf, blobTableName)) {
      LOG.info("No blob meta table:" + blobTableName);
      return;
    }
    CTable ctable = CTable.openTable(conf, blobTableName);
    
    RowFilter rowFilter = new RowFilter(Row.Key.MIN_KEY, Row.Key.MAX_KEY, RowFilter.OP_BETWEEN);
    rowFilter.setPagingInfo(ROW_FETCH_SIZE, null);
    rowFilter.addCellFilter(new CellFilter(Constants.BLOB_COLUMN_FILE));        
    rowFilter.addCellFilter(new CellFilter(Constants.BLOB_COLUMN_DELETE));        
    
    List<MergeFileInfo> targetFileLists = new ArrayList<MergeFileInfo>();

    long sumLength = 0;
    int totalTargetCellCount = 0;
    
    Row[] rows = null;
    while(true) {
      rows = ctable.gets(rowFilter);
      
      if(rows == null || rows.length == 0) {
        if(sumLength >= maxFileLength || totalTargetCellCount >= NUM_CELL_PER_FILE) {
          synchronized(pendingMergeFiles) {
            addToPendingMergeFiles(targetFileLists);
          }
          BlobMerger merger = new BlobMerger(this, zk, conf, tableName, columnName, targetFileLists);
          mergerThreadExecutor.execute(merger);
        } 
        break;
      }

      if(rows.length == 1 && targetFileLists.size() == 0) {
        break;
      }
      
      for(Row eachRow: rows) {
        synchronized(pendingMergeFiles) {
          if(pendingMergeFiles.contains(eachRow.getKey())) {
            continue;
          }
        
          MergeFileInfo mergeFileInfo = makeMergeFileInfo(ctable, eachRow);
          if(mergeFileInfo == null) {
            continue;
          }
          
          if(mergeFileInfo.blobMetaCells.size() == 0 && mergeFileInfo.removedBlobMetaCells.size() == 1) {
            //one file per row, remove record and delete file
            //LOG.info("Delete:" + mergeFileInfo.filePath + ">" + mergeFileInfo.removedBlobMetaCells.get(0).toString());
            BlobMetaManager.removeBlobMeta(conf, mergeFileInfo.filePath, mergeFileInfo.removedBlobMetaCells.get(0));
            fs.delete(new GPath(mergeFileInfo.filePath));
            continue;
          }
          
          //LOG.info("mergeFileInfo.blobMetaCells.size():" + mergeFileInfo.blobMetaCells.size());
          
          if(!mergeFileInfo.isNeedGC(maxFileLength, gcRate) &&
              (mergeFileInfo.totalLength == 0 ||
              mergeFileInfo.totalLength >= maxFileLength ||
              mergeFileInfo.blobMetaCells.size() >= NUM_CELL_PER_FILE) ) {
            //LOG.info("continue:" + mergeFileInfo.isNeedGC(maxFileLength, gcRate) + ">" + mergeFileInfo.totalLength);
            continue;
          }
          sumLength += mergeFileInfo.totalLength;

          targetFileLists.add(mergeFileInfo);
        
          totalTargetCellCount += mergeFileInfo.blobMetaCells.size();
          
          if(sumLength >= maxFileLength || totalTargetCellCount >= NUM_CELL_PER_FILE) {
            addToPendingMergeFiles(targetFileLists);
            BlobMerger merger = new BlobMerger(this, zk, conf, tableName, columnName, targetFileLists);
            mergerThreadExecutor.execute(merger);

            targetFileLists = new ArrayList<MergeFileInfo>();
            sumLength = 0;
            totalTargetCellCount = 0;
          }
        }
      }
      
      rowFilter.setPagingInfo(ROW_FETCH_SIZE, rows[rows.length - 1].getKey());
    }
  }
  
  private void addToPendingMergeFiles(List<MergeFileInfo> mergeFileInfos) {
    for(MergeFileInfo eachFileInfo: mergeFileInfos) {
      pendingMergeFiles.add(new Row.Key(eachFileInfo.filePath));
    }
    
  }
  private MergeFileInfo makeMergeFileInfo(CTable ctable, Row row) throws IOException {
    List<Cell> cells = row.getCellList(Constants.BLOB_COLUMN_FILE);
    if(cells == null || cells.size() == 0) {
      LOG.warn("No data in " + Constants.BLOB_COLUMN_FILE + ": " + row.getKey());
      return null;
    }
    
    Map<Cell.Key, Cell> deleteCellMap = row.getCellMap(Constants.BLOB_COLUMN_DELETE);
    if(deleteCellMap == null || deleteCellMap.isEmpty()) {
      LOG.warn("No data in " + Constants.BLOB_COLUMN_DELETE + ": " + row.getKey());
      
      Row tempRow = ctable.get(row.getKey(), Constants.BLOB_COLUMN_DELETE);
      if(tempRow == null) {
        return null;
      } else {
        List<Cell> tempCells = tempRow.getCellList(Constants.BLOB_COLUMN_DELETE);
        if(tempCells == null || tempCells.size() == 0) {
          return null;
        } else {
          LOG.fatal(" , But " + Constants.BLOB_COLUMN_DELETE + " has data:" + tempCells.size());
          shutdown();
          return null;
        }
      }
    }
      
    long totalLength = 0;
    long removedTotalLength = 0;
    
    List<BlobMetaCell> blobMetaCells = new ArrayList<BlobMetaCell>();
    List<BlobMetaCell> deletedBlobMetaCells = new ArrayList<BlobMetaCell>();
    
    for(Cell eachCell: cells) {
      Cell.Key cellKey = eachCell.getKey();
      Cell deleteCell = deleteCellMap.get(cellKey);
      
      BlobMetaCell blobMetaCell = new BlobMetaCell();
      blobMetaCell.readFields(new DataInputStream(new ByteArrayInputStream(eachCell.getBytes())));

      if(deleteCell == null || "Y".equals(deleteCell.getValueAsString())) {
        removedTotalLength += blobMetaCell.getLength();
        deletedBlobMetaCells.add(blobMetaCell);
      } else {
        totalLength += blobMetaCell.getLength();
        blobMetaCells.add(blobMetaCell);
      }
    }
    return new MergeFileInfo(row.getKey().toString(), totalLength, removedTotalLength, blobMetaCells, deletedBlobMetaCells);
  }
  
  public static class MergeFileInfo {
    String filePath;
    
    long totalLength;
    long removedTotalLength;
    
    List<BlobMetaCell> blobMetaCells;
    List<BlobMetaCell> removedBlobMetaCells;
    
    public MergeFileInfo(String filePath, long totalLength, long removedTotalLength, 
        List<BlobMetaCell> blobMetaCells, List<BlobMetaCell> removedBlobMetaCells) {
      this.blobMetaCells = blobMetaCells;
      this.removedBlobMetaCells = removedBlobMetaCells;
      this.filePath = filePath;
      this.totalLength = totalLength;
      this.removedTotalLength = removedTotalLength;
    }
    
    public boolean isNeedGC(long maxFileLength, float gcRate) {
      int live = blobMetaCells.size();
      int removed = removedBlobMetaCells.size();

      float rate = (float)removed/((float)live + (float)removed);
      //LOG.debug(live + "," + removed + "," + totalLength + "," + removedTotalLength + "," + rate);
      return ((totalLength + removedTotalLength) >= maxFileLength || (live + removed) >= NUM_CELL_PER_FILE) && rate >= gcRate;
    }

    public int hashCode() {
      return filePath.hashCode();  
    }
    
    public boolean equals(Object o) {
      MergeFileInfo target = (MergeFileInfo)o;
      
      return filePath.equals(target.filePath);
    }
  }
  
  public static void main(String[] args) throws Exception {
    BlobManager blobManager = new BlobManager(new CloudataConf());
    blobManager.start();
  }
}
