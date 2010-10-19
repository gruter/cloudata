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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CellFilter;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.fs.CloudataFileSystem;
import org.cloudata.core.fs.GPath;
import org.cloudata.core.tablet.ColumnValue;
import org.cloudata.core.tablet.TabletInfo;


public class TabletMapFile {
  public static final Log LOG = LogFactory.getLog(TabletMapFile.class.getName());
  
  public static final String DATA_FILE = "map.dat";
  public static final String IDX_FILE = "map.idx";

  private String fileId;
  
  protected GPath filePath;
  
  private SortedSet<MapFileIndexRecord> mapFileIndexRecords = new TreeSet<MapFileIndexRecord>();

  private CloudataConf conf;
  
  protected CloudataFileSystem fs;

  private TabletInfo tabletInfo;
  
  private String columnName;
  
  private int numOfVersion;
  
//  private boolean closed = false;
  //private LocatedBlocks dfsBlockInfos;
  
  public TabletMapFile(
      CloudataConf conf,
      CloudataFileSystem fs,
      TabletInfo tabletInfo, 
      String columnName,
      String fileId, 
      GPath filePath,
      int numOfVersion) throws IOException {
    this.conf = conf;
    this.fs = fs;
    this.filePath = filePath;
    this.fileId = fileId;
    this.tabletInfo = tabletInfo;
    this.columnName = columnName;
    this.numOfVersion = numOfVersion;
  }

  public TabletMapFile(CloudataConf conf, TabletInfo tabletInfo, String columnName, String fileId, int numOfVersion) throws IOException  {
    this(conf, CloudataFileSystem.get(conf),
        tabletInfo, 
        columnName, 
        fileId, 
        new GPath(Tablet.getTabletPath(conf, tabletInfo), columnName + "/" + fileId + "/"),
        numOfVersion);
  }

  public TabletInfo getTabletInfo() {
    return tabletInfo;
  }
  
  public String getColumnName() {
    return columnName;
  }
  
  public MapFileWriter getMapFileWriter() throws IOException {
    return getMapFileWriter(0);
  }
  
  public MapFileWriter getMapFileWriter(int bufferSize) throws IOException {
    int indexBlockSize = conf.getInt("mapfile.indexBlockSize", 128) * 1024;
    return (bufferSize > 0) ? new MapFileWriter(fs, filePath, mapFileIndexRecords, indexBlockSize, bufferSize) :
      new MapFileWriter(fs, filePath, mapFileIndexRecords, indexBlockSize);
  }

  public MapFileReader getMapFileReader(Row.Key startRowKey, Row.Key endRowKey, CellFilter cellFilter) throws IOException {
    return new MapFileReader(startRowKey, endRowKey, cellFilter);
  }

  public MapFileReader getMapFileReader(Row.Key startRowKey, Row.Key endRowKey) throws IOException {
    return getMapFileReader(startRowKey, endRowKey, new CellFilter());
  }
  
  public MapFileReader getMapFileReader(Row.Key startRowKey, Row.Key endRowKey, Cell.Key cellKey) throws IOException {
    CellFilter cellFilter = new CellFilter();
    cellFilter.setCellKey(cellKey);
    return getMapFileReader(startRowKey, endRowKey, cellFilter);
  }
  
  public MapFileReader getMapFileReader() throws IOException {
    return getMapFileReader(new CellFilter());
  }

  public MapFileReader getMapFileReader(CellFilter cellFilter) throws IOException {
    return getMapFileReader(Row.Key.MIN_KEY, Row.Key.MAX_KEY, cellFilter);
  }

  public boolean loadIndex() throws IOException {
    DataInputStream in = null;
    
    GPath idxPath = new GPath(filePath, IDX_FILE);
    GPath dataPath = new GPath(filePath, DATA_FILE);
    try {
      int fileCount = 0;
      if(fs.exists(idxPath))    fileCount++;
      if(fs.exists(dataPath))   fileCount++;
      if(fileCount == 0) {
        return false;
      } else if(fileCount == 1) {
        LOG.error("data or idx file not exists. tabletInfo=" + tabletInfo + ", column=" + columnName + ", fileId=" + fileId);
        return false;
      }
      
      long dataFileLength = fs.getLength(new GPath(filePath, DATA_FILE));
      boolean idxFileError = false;
      long lastIndexOffset = 0;
      
      //LOG.debug("loadIndex:" + idxPath);
      in = new DataInputStream(fs.open(idxPath)); 
      MapFileIndexRecord mapFileIndexRecord = null;
      try {
        while(true) {
          mapFileIndexRecord = new MapFileIndexRecord();
          mapFileIndexRecord.read(in);
          if(mapFileIndexRecord.getOffset() > dataFileLength) {
            idxFileError = true;
            lastIndexOffset = mapFileIndexRecord.getOffset();
            break;
          }        
          mapFileIndexRecords.add(mapFileIndexRecord);
        }
      } catch (EOFException eof) {
        
      }
      
      if(idxFileError) {
        throw new IOException("Can't load map file. index offset(" + lastIndexOffset + 
                                  ") greate than data file length(" + dataFileLength + ").[path=" + filePath + "]");
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new IOException(e.getMessage());
    } finally {
      if(in != null)  in.close();
    }
    
//    try {
//      synchronized(this) {
//        dfsBlockInfos = fs.getBlockInfos(dataPath);
//      }
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
    return true;
  }
  
  public long getIndexMemorySize() throws IOException {
    long result = 0;
    for(MapFileIndexRecord mapFileIndexRecord: mapFileIndexRecords) {
      result += mapFileIndexRecord.getMemorySize();
    }
    return result;
  }
  
  class MapFileReader implements Scanner, Searchable {
    private Row.Key currentRowKey;
    private DataInputStream dataIn;
    private DataInputStream seekableIn;
    
    private Row.Key startRowKey;
    private Row.Key endRowKey;

    private CellFilter cellFilter;
    
    private GPath mapFilePath;
    
    public MapFileReader(Row.Key startRowKey, Row.Key endRowKey, CellFilter cellFilter) throws IOException {
      this.startRowKey = startRowKey;
      this.endRowKey = endRowKey;
      this.cellFilter = cellFilter;

      MapFileIndexRecord indexRecord = findNearest(startRowKey, cellFilter.getTargetCellKey());
      
      if(indexRecord != null) {
        mapFilePath = new GPath(filePath, DATA_FILE);
        try {
          seekableIn = fs.openDataInputStream(mapFilePath);
        } catch (IOException e) {     
          LOG.warn("Error in creating MapFileReader: " + filePath + ": " +  e.getMessage());
          throw e;
        }
        fs.seek(seekableIn, indexRecord.getOffset());
        dataIn = seekableIn;
      } else {
        seekableIn = null;
      }
    }
    
    public ColumnValue next() throws IOException {
      if(dataIn == null) {
        return null;
      }
      while(true) {
        MapFileColumnValue mapFileColumnValue = new MapFileColumnValue();
        try {
          mapFileColumnValue.readFields(dataIn);
        } catch (EOFException eof) {
          return null;
        }
        currentRowKey = mapFileColumnValue.getRowKey();
        
        if(currentRowKey.compareTo(endRowKey) > 0) {
          return null;
        }
        
        if(currentRowKey.compareTo(startRowKey) < 0) {
          mapFileColumnValue = skipToTargetRow();
          if(mapFileColumnValue == null) {
            currentRowKey = null;
            return null;
          }
          currentRowKey = mapFileColumnValue.getRowKey();
        } 
        
        if(cellFilter.matchKey(mapFileColumnValue.getCellKey())) {
          ColumnValue columnValue = new ColumnValue();
          copyColumnValue(mapFileColumnValue, columnValue);
          return columnValue;
        } 
      }
    }

    private MapFileColumnValue skipToTargetRow() throws IOException {
      MapFileColumnValue mapFileColumnValue = new MapFileColumnValue();
      try {
        while(true) {
          mapFileColumnValue.readFields(dataIn);
          Row.Key rowKey = mapFileColumnValue.getRowKey();

          if(rowKey.compareTo(startRowKey) < 0) {
            continue;
          } else if(rowKey.compareTo(startRowKey) >= 0 && rowKey.compareTo(endRowKey) <= 0) {
            return mapFileColumnValue;
          } else if(rowKey.compareTo(endRowKey) > 0) {
            return null;
          }
        }
      } catch (EOFException eof) {
      }      
      return null;
    }
    
    public Row.Key getCurrentRowKey() {
      return currentRowKey;
    }

    public void close() throws IOException {
      if(dataIn != null)  dataIn.close();
      dataIn = null;
    }

    public int avaliable() throws IOException {
      if(dataIn == null)  return 0;
      return dataIn.available();
    }
  }
  
  public static class MapFileWriter {
    private DataOutputStream dataOut;
    private DataOutputStream indexOut;  
    private CloudataFileSystem fs;
    
    private ColumnValue lastColumnRecord;
    
    private int blockWriteBytes = 0;
    private long currentDataFilePos = 0;
    private long indexFileOffset = 0;
    
    //FIXME 성능 테스트 후 사이즈 조정
    private int indexBlockSize;
    private final int BLOCK_RECORD_COUNT = 300;
    
    private int blockWriteRecordCount = 0;
    
    private long sumDataLength = 0;
    private long sumIndexLength = 0;
    
    //private long writeTotalTime = 0;
    SortedSet<MapFileIndexRecord> mapFileIndexRecords;
    
    public MapFileWriter(CloudataFileSystem fs, GPath filePath, 
        SortedSet<MapFileIndexRecord> mapFileIndexRecords, int indexBlockSize) throws IOException {
      this(fs, filePath, mapFileIndexRecords, indexBlockSize, 0);
    }
    
    public MapFileWriter(CloudataFileSystem fs, GPath filePath, SortedSet<MapFileIndexRecord> mapFileIndexRecords,
        int indexBlockSize, int outputBufferSize) throws IOException {
      this.indexBlockSize = indexBlockSize;
      this.fs = fs;
      this.mapFileIndexRecords = mapFileIndexRecords;
      fs.mkdirs(filePath);

      dataOut = (outputBufferSize > 0) ?
      new DataOutputStream(fs.create(new GPath(filePath, DATA_FILE), outputBufferSize)) :
        new DataOutputStream(fs.create(new GPath(filePath, DATA_FILE)));
      indexOut = new DataOutputStream(fs.create(new GPath(filePath, IDX_FILE)));
	}

	public int write(ValueCollection columnValues) throws IOException {
      if(columnValues.isEmpty()) {
        return 0;
      }
      ColumnValue columnValue = columnValues.getWithDeleted();
      
      //long startTime = System.currentTimeMillis();
      int writeSize = columnValues.write(dataOut);
      //writeTotalTime += (System.currentTimeMillis() - startTime);
      sumDataLength += writeSize;
      
      makeFileIndex(writeSize, columnValue, indexBlockSize);
      
      lastColumnRecord = columnValue;
      
      return writeSize;
    }
    
    public int write(ColumnValue columnValue) throws IOException {
      MapFileColumnValue mapFileColumnValue = new MapFileColumnValue();
      ValueCollection.copyMapFileColumnValue(columnValue, mapFileColumnValue);
      mapFileColumnValue.write(dataOut);
      int writeSize = mapFileColumnValue.size();
      
      makeFileIndex(writeSize, columnValue, indexBlockSize);
      
      lastColumnRecord = columnValue;
      //dataOut.flush();
      //indexOut.flush();
      return writeSize;
    }

    private void makeFileIndex(int writeSize, ColumnValue columnValue, int checkSize) throws IOException {
      blockWriteBytes += writeSize;
      currentDataFilePos += writeSize;
      blockWriteRecordCount++;
      
      //누적된 writeBytes가 block size(64kb)보다 큰 경우 현재의 레코드를 index에 추가
      if(blockWriteBytes > checkSize || blockWriteRecordCount > BLOCK_RECORD_COUNT) {
        //index 파일에 index 레코드 추가
        MapFileIndexRecord mapFileIndexRecord = new MapFileIndexRecord(columnValue.getRowKey(), columnValue, indexFileOffset);
        mapFileIndexRecord.write(indexOut);
        mapFileIndexRecords.add(mapFileIndexRecord);
        
        sumIndexLength += mapFileIndexRecord.getMemorySize();
        
        blockWriteBytes = 0;
        blockWriteRecordCount = 0;
        indexFileOffset = currentDataFilePos;
      } 
    }
    
    public void close() throws IOException {
      if(dataOut == null) {
        return;
      }
//      if (closed) {
//        return;
//      }

      if(blockWriteBytes > 0) {
        makeFileIndex(0, lastColumnRecord, 0);
      }
      
      dataOut.flush();
      indexOut.flush();
            
      IOException exception = null;
      
      if(dataOut != null) {
        try {
          dataOut.close();
          dataOut = null;
          //fs.setReplication(new GPath(filePath, DATA_FILE), (short)3);
        } catch (IOException e) { 
          LOG.error(e.getMessage() + ", sumDataLength=" + sumDataLength, e);
          exception = e;
        }
      }
      if(indexOut != null) {
        try {
          indexOut.close();
          indexOut = null;
        } catch (IOException e) { 
          LOG.error(e.getMessage() + ", sumIndexLength=" + sumIndexLength, e);
          exception = e;
        }
      }
//      closed = true;

      if(exception != null) {
        throw exception;
      }      
    }
  }

  public ValueCollection scan(Row.Key rowKey, Cell.Key columnKey) throws IOException {
    MapFileReader reader = null;
    try {
      reader = getMapFileReader(rowKey, new Row.Key(Constants.MAX_VALUE), columnKey);
  
      ValueCollection valueCollection = new ValueCollection();
      ColumnValue columnValue = null;
      
      while( (columnValue = reader.next()) != null ) {
        Row.Key scanRowKey = columnValue.getRowKey();
        if(scanRowKey.compareTo(rowKey) > 0) {
          break;
        }
        
        if(scanRowKey.equals(rowKey)) {
          if(columnKey == null) {
            valueCollection.add(columnValue, numOfVersion);
          } else {
            if(columnValue.getCellKey().compareTo(columnKey) == 0) {
              valueCollection.add(columnValue, numOfVersion);
            } else if(columnValue.getCellKey().compareTo(columnKey) > 0) {
              break;
            } else {
              
            }
          }
        }
      }
      return valueCollection;
    } finally {
      reader.close();
    }
  }
  

  public List<ColumnValue> scan(Row.Key rowKey) throws IOException {
    MapFileReader reader = null;
    try {
      reader = getMapFileReader(rowKey, rowKey);

      List<ColumnValue> result = new ArrayList<ColumnValue>();
      ColumnValue columnValue = null;
      
      while ((columnValue = reader.next()) != null) {
        result.add(columnValue);
      }
      return result;
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }
  
  public boolean hasValue(Row.Key rowKey, Cell.Key cellKey) throws IOException {
    MapFileReader reader = null;
    try {
      reader = getMapFileReader(rowKey, rowKey, cellKey);
      
      ColumnValue columnValue = null;
      
      while( (columnValue = reader.next()) != null ) {
        if(columnValue.getRowKey().compareTo(rowKey) < 0) {
          continue;
        } 
        
        if(cellKey != null && columnValue.getCellKey().compareTo(cellKey) < 0) {
          continue;
        }
        
        if(columnValue.getRowKey().compareTo(rowKey) > 0) {
          return false;
        } else {
          if(cellKey != null && columnValue.getCellKey().compareTo(cellKey) > 0) {
            return false;
          }
          return true;
        }
      }
      return false;
    } finally {
      reader.close();
    }
  }
  
  public MapFileIndexRecord findNearest(Row.Key targetRowKey, Cell.Key cellKey) throws IOException {
    SortedSet<MapFileIndexRecord> tailMap = mapFileIndexRecords.tailSet(new MapFileIndexRecord(targetRowKey, cellKey));
    if(tailMap.size() > 0) {
      return tailMap.first();
    } else {
      return null;
    }
  }

  public GPath getFilePath() {
    return filePath;
  } 
  
  public void moveTo(GPath targetPath, boolean deleteParent) throws IOException { GPath originPath = this.filePath;
    GPath originMapDataPath = new GPath(originPath, DATA_FILE);
    GPath originMapIndexPath = new GPath(originPath, IDX_FILE);

    GPath destDataPath = new GPath(targetPath, DATA_FILE);
    GPath destIndexPath = new GPath(targetPath, IDX_FILE);
    
    //LOG.info("move data file: " + originMapDataPath + " to " + destDataPath);
    if(!fs.move(originMapDataPath, destDataPath)) {
      LOG.error("Can't move data file: " + originMapDataPath + " to " + destDataPath);
      throw new IOException("Can't move data file: " + originMapDataPath + " to " + destDataPath);
    }

    //LOG.debug("move index file: " + originMapIndexPath + " to " + destIndexPath);
    if(!fs.move(originMapIndexPath, destIndexPath)) {
      LOG.error("Can't move index file: " + originMapIndexPath + " to " + destIndexPath);
      throw new IOException("Can't move index file: " + originMapIndexPath + " to " + destIndexPath);
    }

    //delete temp dir
    if(deleteParent) {
      int retry = 0;
      while(retry < 5) {
        //LOG.debug("minor compaction temp dir:" + originPath);
        boolean result = fs.delete(originPath, true);
        if(result) {
          break;
        }
        
        LOG.info("fail to delete file [" + originPath + "] but retry");
        
        retry++;
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          return;
        }
      }
      if(retry >= 5) {
        LOG.warn("Can't delete minor compaction temp dir:" + originPath);
      }
      //LOG.debug("moveTo:delete:" + originMapIndexPath + " to " + destIndexPath);
    }
    this.filePath = targetPath;
  }

  public boolean delete() throws IOException {
    int retry = 0;
    while(retry < 5) {
      boolean result = fs.delete(filePath, true);
      if(result) {
        return true;
      }
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
      }
    }
    return false;
  } 
  
  public boolean equals(Object obj) {
    if(!(obj instanceof TabletMapFile)) return false;
    
    TabletMapFile otherObj = (TabletMapFile)obj;
    
    
    if(!tabletInfo.getTabletName().equals(otherObj.tabletInfo.getTabletName()))     return false;
    if(!columnName.equals(otherObj.columnName))     return false;
    return fileId.equals(otherObj.fileId);
  }

  public String toString() {
    return "[tabletName=" + tabletInfo.getTabletName() + ", columnName=" + columnName + ", path=" + filePath; 
  }
  
//  public void printIndex() throws IOException {
//    System.out.println("\t\tMapFile: " + filePath);
//    for(MapFileIndexRecord indexRecord: mapFileIndexRecords) {
//      System.out.println("\t\t\t" + indexRecord);
//    }
//  }

  public SortedSet<MapFileIndexRecord> getMapFileIndexRecords() {
    return mapFileIndexRecords;
  }

  public long getDataFileSize() throws IOException {
    return fs.getLength(new GPath(filePath, DATA_FILE));
  }

  /**
   * @return the fileId
   */
  public String getFileId() { 
    return fileId;
  }


  public CloudataFileSystem getFileSystem() {
    return fs;
  }
  
  public static void copyColumnValue(MapFileColumnValue mapFileColumnValue, ColumnValue columnValue) {
    columnValue.setRowKey(mapFileColumnValue.getRowKey());
    columnValue.setCellKey(mapFileColumnValue.getCellKey());
    columnValue.setValue(mapFileColumnValue.getValue());
    columnValue.setTimestamp(mapFileColumnValue.getTimestamp());
    columnValue.setDeleted(mapFileColumnValue.isDeleted());
  }
  
  public static void main(String[] args) throws IOException {
    if(args.length < 4) {
      System.out.println("Usage: java TabletMapFile <table name> <tablet name> <column name> <file id>");
      System.exit(0);
    }
    
    TabletInfo tabletInfo = new TabletInfo();
    tabletInfo.setTableName(args[0]);
    tabletInfo.setTabletName(args[1]);

    TabletMapFile mapFile = new TabletMapFile(new CloudataConf(), tabletInfo, args[2], args[3], 0);
    
    mapFile.loadIndex();
    
    MapFileReader reader = mapFile.getMapFileReader(Row.Key.MIN_KEY, Row.Key.MAX_KEY);
    try {
      ColumnValue columnValue = null;
      while( (columnValue = reader.next()) != null ) {
        System.out.println(columnValue.getRowKey() + "," + columnValue.getCellKey() + "," + new String(columnValue.getValue()));
        /*
	try {
	  TabletInfo tabletInfo1 = new TabletInfo();
	  if(columnValue.getValue() == null) {
        	System.out.println("[" + columnValue.getRowKey() + "][" + columnValue.getCellKey() + 
            		"][" + columnValue.getTimestamp() + "][" + columnValue.isDeleted() + "][" + 
            		(columnValue.getValue() == null ? "null" : columnValue.getValue().length) + "]");
		continue;
	  }
	  tabletInfo1.readFields(columnValue.getValue());
          System.out.println("[" + columnValue.getRowKey() + "][" + columnValue.getCellKey() + 
              "][" + columnValue.getTimestamp() + "][" + columnValue.isDeleted() + "][" + 
            (columnValue.getValue() == null ? "null" : columnValue.getValue().length) + "]" + tabletInfo1);
	} catch (Exception e) {
		System.out.println("Error:" + columnValue.getRowKey() + "][" + columnValue.getCellKey());
	}
        */
      }
    } finally {
      reader.close();
    }
  }
}
  
