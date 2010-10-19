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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CellFilter;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.RowFilter;
import org.cloudata.core.client.blob.NBlobInputStream;
import org.cloudata.core.client.scanner.ScannerFactory;
import org.cloudata.core.client.scanner.TableScanner;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.lock.LockUtil;
import org.cloudata.core.tablet.TableSchema;
import org.cloudata.core.tablet.TableSchema.ColumnInfo;


/**
 * @author jindolk
 *
 */
public class TestBlobManyFile {
  public static final String TABLE_NAME = "T_BLOB_TEST";
  public static final String COL1 = "FILE_PATH";
  public static final String COL2 = "FILE_DATA";
  
  public static void main(String[] args) throws IOException {
    if(args.length < 1) {
      usage();
      System.exit(0);
    }
    
    if("upload".equals(args[0])) {
      (new BlobUploader(args[1])).start();
    } else if("read".equals(args[0])) {
      (new BlobReader()).start();
    } else if("delete".equals(args[0])) {
      (new BlobDelete()).start();
    } else {
      usage();
      System.exit(0);
    }
  } 
  
  public static void usage() {
    System.out.println("Usage: java TestBlobManyFile <cmd> [options]");
    System.out.println("cmd:");
    System.out.println("  upload <top path>");
    System.out.println("  delete");
    System.out.println("  read");
  }
}

class BlobUploader {
  Log LOG = LogFactory.getLog(BlobUploader.class.getName());
  String topPath;
  String topPathName;
  long rowKeySeq;
  CloudataConf conf;
  CTable ctable;
  
  public BlobUploader(String topPath) {
    this.topPath = topPath;
    this.conf = new CloudataConf();
  }
  
  public void start() {
    try {
      if(!CTable.existsTable(conf, TestBlobManyFile.TABLE_NAME)) {
        TableSchema tableSchema = new TableSchema(TestBlobManyFile.TABLE_NAME);
        tableSchema.addColumn(new ColumnInfo(TestBlobManyFile.COL1));
        tableSchema.addColumn(new ColumnInfo(TestBlobManyFile.COL2, TableSchema.BLOB_TYPE));
        
        try {
          CTable.createTable(conf, tableSchema);
        } catch (Exception e) {
          LOG.warn(e);
        }
      }
      ctable = CTable.openTable(conf, TestBlobManyFile.TABLE_NAME); 
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      e.printStackTrace();
      System.exit(0);
    }
    
    File file = new File(topPath);
    topPathName = file.getName();
    
    if(!file.isDirectory()) {
      LOG.fatal(topPath + " is not directory. BlobUploader stoped.");
      System.exit(0);
    }
    
    while(true) {
      File[] files = file.listFiles();
      if(files == null || files.length == 0) {
        LOG.fatal("No files or directory in " + topPath + ". BlobUploader stoped.");
        System.exit(0);
      }
      
      for(File eachFile: files) {
        upload(eachFile);
      }
    }
  }
  
  private void upload(File parentPath) {
    File[] files = parentPath.listFiles();
    
    if(files == null || files.length == 0) {
      return;
    }
      
    DecimalFormat df = new DecimalFormat("000000000000000");
    try {
      for(File eachFile: files) {
        if(eachFile.isFile()) {
          uploadFile(eachFile, topPathName + "_" + df.format(rowKeySeq));
          rowKeySeq++;
          if(rowKeySeq % 100 == 0) {
            LOG.info(rowKeySeq + " file uploaded, " + topPath);
          }
        } else {
          upload(eachFile);
        }
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      e.printStackTrace();
      System.exit(0);
    }
  }
  
  private void uploadFile(File file, String rowKeyStr) throws IOException {
    Row.Key rowKey = new Row.Key(rowKeyStr);
    
    Row row = new Row(rowKey);
    row.addCell(TestBlobManyFile.COL1, new Cell(Cell.Key.EMPTY_KEY, file.getPath().getBytes()));
    
    ctable.put(row);
    
    BufferedOutputStream out = new BufferedOutputStream(
        ctable.createBlob(rowKey, TestBlobManyFile.COL2, Cell.Key.EMPTY_KEY));
    
    BufferedInputStream in = new BufferedInputStream(new FileInputStream(file));
    
    try {
      byte[] buf = new byte[10 * 1024];
      
      int readByte = 0;
      
      while( (readByte = in.read(buf)) > 0 ) {
        out.write(buf, 0 , readByte);
      }
    } finally {
      if(out != null) {
        out.close();
      } 
      if(in != null) {
        in.close();
      }
    }
  }
}

class BlobDelete {
  Log LOG = LogFactory.getLog(BlobDelete.class.getName());
  CTable ctable;
  CloudataConf conf;
  ZooKeeper zk;
  String testLockPath = "BLOB_TEST";
  
  public BlobDelete() {
    conf = new CloudataConf();
  }
  
  public void start() {
    LOG.info("BlobDelete started");
    try {
      zk = LockUtil.getZooKeeper(conf, "BlobDelete_" + System.currentTimeMillis() , null);
      if(zk.exists(LockUtil.getZKPath(conf, testLockPath), null) == null) {
        LockUtil.createNodes(zk, LockUtil.getZKPath(conf, testLockPath), null, CreateMode.PERSISTENT);
      }
      ctable = CTable.openTable(conf, TestBlobManyFile.TABLE_NAME);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      e.printStackTrace();
      System.exit(0);
    }
    
    while(true) {
      RowFilter rowFilter = new RowFilter(Row.Key.MIN_KEY, Row.Key.MAX_KEY, RowFilter.OP_BETWEEN);
      rowFilter.addCellFilter(new CellFilter(TestBlobManyFile.COL1));
      rowFilter.setPagingInfo(10, null);
      
      List<Row.Key> deleteTargetRowKeys = new ArrayList<Row.Key>();
      
      TableScanner scanner = null;
      try {
        scanner = ScannerFactory.openScanner(ctable, rowFilter);
        
        Row row = null;
        int index = 1;
        while( (row = scanner.nextRow()) != null ) {
          if(index % 20 == 0) { //delete 5%
            deleteTargetRowKeys.add(row.getKey());
          }
          index++;
        }
        scanner.close();
        scanner = null;
        
        LOG.info("deleting " + deleteTargetRowKeys.size());
        delete(deleteTargetRowKeys);
        LOG.info(deleteTargetRowKeys.size() + " deleted");
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
        e.printStackTrace();
        if(scanner != null) {
          try {
            scanner.close();
          } catch (IOException e1) {
          }
          scanner = null;
        }
        System.exit(0);
      }
      finally {
        if(scanner != null) {
          try {
            scanner.close();
          } catch (IOException e1) {
          }
          scanner = null;
        }
      }
    }
  }
  
  private void delete(List<Row.Key> deleteTargets) throws IOException {
    for(Row.Key eachRowKey: deleteTargets) {
      //zk lock
      String lockPath = LockUtil.getZKPath(conf, testLockPath);
      //LOG.info("try delete lock path:" + lockPath);
      String lockKey = LockUtil.acquireLock(zk, lockPath);
      //LOG.info("after delete lock path:" + lockPath);
      
      try {
        //delete
        ctable.removeRow(eachRowKey);
        try {
          Thread.sleep(2 * 1000);
        } catch (InterruptedException e) {
          return;
        }
      } finally {
        //zk unlock
        LockUtil.releaseLock(zk, lockPath, lockKey);
      }
    }
  }
}

class BlobReader {
  Log LOG = LogFactory.getLog(BlobReader.class.getName());  
  CTable ctable;
  CloudataConf conf = new CloudataConf();
  
  public void start() {
    while(true) {
      LOG.info("Start Compare");
      read();
      LOG.info("End Compare");
    }
  }
  
  public void read() {
    try {
      ctable = CTable.openTable(conf, TestBlobManyFile.TABLE_NAME);
      
      RowFilter rowFilter = new RowFilter(Row.Key.MIN_KEY, Row.Key.MAX_KEY, RowFilter.OP_BETWEEN);
      rowFilter.addCellFilter(new CellFilter(TestBlobManyFile.COL1));
      rowFilter.addCellFilter(new CellFilter(TestBlobManyFile.COL2));
      rowFilter.setPagingInfo(10, null);
      
      int count = 0;
      while(true) {
        Row[] rows = ctable.gets(rowFilter);
        
        if(rows == null || rows.length == 0) {
          break;
        }
        for(Row eachRow: rows) {
          Map<Cell.Key, Cell> cells = eachRow.getCellMap(TestBlobManyFile.COL2);
          if(cells == null || cells.size() == 0) {
            //Stream에 close 되기전에는 BLOB_COLUMN 정보는 저장되지 않는다.
            continue;
          }

          List<Cell> localPathCells = eachRow.getCellList(TestBlobManyFile.COL1);
          if(localPathCells == null || localPathCells.size() == 0) {
            continue;
          }
          
          for(Cell eachCell: localPathCells) {
            if(!cells.containsKey(eachCell.getKey())) {
              continue;
            }
            
            String localPath = eachCell.getValueAsString();
            
            NBlobInputStream blobIn = null;
            try {
              blobIn = (NBlobInputStream)ctable.openBlob(
                  eachRow.getKey(), TestBlobManyFile.COL2, eachCell.getKey());
              compareFile(localPath, blobIn);
              count++;
            } catch (Exception e) {
              LOG.error(localPath + "," + eachRow.getKey() + "," + eachCell.getKey() + ":" + e.getMessage(), e);
              //e.printStackTrace();
              continue;
            } finally {
              if(blobIn != null) {
                blobIn.close();
              }
            }
          }
        }
          
        rowFilter.setPagingInfo(10, rows[rows.length - 1].getKey());
      }
      
      LOG.info(count + " file compared");
      Thread.sleep(10 * 1000);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      e.printStackTrace();
    }
  }  
  
  private void compareFile(String localPath, NBlobInputStream blobStream) throws IOException {
    File file = new File(localPath);
    
    if(file.length() != blobStream.getLength()) {
      throw new IOException("Different length: " + file.length() + "," + blobStream.getLength());
    }
    
    BufferedInputStream localStream = new BufferedInputStream(new FileInputStream(file));  
    BufferedInputStream bufStream = new BufferedInputStream(blobStream); 
    try {
      byte[] localBuf = new byte[1024];
      byte[] blobBuf = new byte[1024];

      int localRead = 0;
      int blobRead = 0;
      
      long localTotalRead = 0;
      
      while( (localRead = localStream.read(localBuf)) > 0) {
        blobRead = bufStream.read(blobBuf);

        if(localRead != blobRead) {
          throw new IOException("Different read bytes: " + localRead + "," + blobRead + ", totalRead=" + localTotalRead);
        }
        for(int i = 0; i < localBuf.length; i++) {
          if (localBuf[i] != blobBuf[i]) {
            throw new IOException("Different byte: " + (localTotalRead + i));
          }
        }
        localTotalRead += blobRead;
      }
      
      if(blobStream.available() > 0) {
        throw new IOException("Local stream finished, but blob stream remain:" + blobStream.available());
      }
    } finally {
      if(localStream != null) {
        localStream.close();
      }
    }
  }
}
