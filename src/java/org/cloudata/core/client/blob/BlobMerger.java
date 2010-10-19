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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.cloudata.core.client.blob.BlobManager.MergeFileInfo;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.lock.LockUtil;
import org.cloudata.core.fs.CloudataFileSystem;
import org.cloudata.core.fs.GPath;


/**
 * @author jindolk
 *
 */
public class BlobMerger extends Thread {
  public static final Log LOG = LogFactory.getLog(BlobMerger.class.getName());
  
  private String tableName;
  private String columnName;
  private List<MergeFileInfo> mergeFileInfos;
  private CloudataConf conf;
  private CloudataFileSystem fs;
  private BlobManager blobManager;
  private Random rand;
  private ZooKeeper zk;
  private boolean existsParent;
  
  public BlobMerger(BlobManager blobManager, ZooKeeper zk, CloudataConf conf, 
      String tableName, String columnName, List<MergeFileInfo> mergeFileInfos) {
    this.conf = conf;
    this.mergeFileInfos = mergeFileInfos;
    
    this.tableName = tableName;
    this.columnName = columnName;
    
    this.fs = CloudataFileSystem.get(conf);
    
    this.blobManager = blobManager;
    
    this.rand = new Random(System.currentTimeMillis());
    
    this.zk = zk;
  }

  public void run() {
    LOG.info("BlobMerger started: " + tableName + "," + columnName + ", #files:" + mergeFileInfos.size());
    
    try {
      String newFilePath = merge();

      blobManager.addPendingMergeFile(newFilePath);
      
      modifyBlobMeta(newFilePath);
      
      List<String> paths = new ArrayList<String>();
      for(MergeFileInfo eachFileInfo: mergeFileInfos) {
        paths.add(eachFileInfo.filePath);
      }
      
      blobManager.endMerge(paths);
    } catch(InterruptedException e) {
      return;
    } catch (Exception e) {
      e.printStackTrace();
      blobManager.error(e);
    }
  }
  
  private String merge() throws Exception {
    BufferedOutputStream out = null;
    String fileName = (System.currentTimeMillis() +  System.nanoTime()) + "" + rand.nextInt();
    
    GPath tempPath = new GPath(BlobMetaManager.getBlobTempPath(conf, tableName, columnName) + "/" + fileName);
    GPath realPath = new GPath(BlobMetaManager.getBlobRootPath(conf, tableName, columnName) + "/" + fileName);
    
    try {
      byte[] buf = new byte[100 * 1024];
      
      long prevOffset = 0;
      long offset = 0;

      int mergeTargetFileCount = 0;
      long mergeTargetFileLength = 0;
      
      out = new BufferedOutputStream(fs.create(tempPath));
      try {
        for(MergeFileInfo eachFileInfo: mergeFileInfos) {
          for(BlobMetaCell eachMetaCell: eachFileInfo.blobMetaCells) {
            BufferedInputStream in = null;
            try {
              //merge 중에 다른 클라이언트가 삭제한 경우 File을 오픈하면 IOException이 발생한다.
              //이 경우 BLOB 테이블의 데이터를 읽어 삭제된 경우에는 삭제 처리를 수행하고
              //그렇지 않으면 오류를 throws 한다.
              try {
                in = new BufferedInputStream(
                        new NBlobInputStream(conf, eachFileInfo.filePath, eachMetaCell));
              } catch(AlreadyDeletedException e) {
                LOG.info(e.getMessage());
                continue;
              }
              mergeTargetFileCount++;
              mergeTargetFileLength += eachMetaCell.getLength();
              
              int readBytes = 0;
              long totalRead = 0;
              while( (readBytes = in.read(buf)) > 0 ) {
                out.write(buf, 0, readBytes);
                offset += readBytes;
                totalRead += readBytes;
              }
              
              if(totalRead != eachMetaCell.getLength()) {
                throw new IOException(eachMetaCell.getRowKey() + "," + eachMetaCell.getCellKey() + 
                    " different file length:" + eachMetaCell.getLength() + "," + totalRead);
              }
            } catch (Exception e) {
              LOG.error("Merge Error: " + eachMetaCell.getRowKey() + "," + eachMetaCell.getCellKey(), e);
              throw e;
            } finally {
              if(in != null) {
                in.close();
              }
            }
            eachMetaCell.setOffset(prevOffset);
            prevOffset = offset;
          }
          
          deleteRemovedFiles(eachFileInfo);
        }
      } finally {
        if(out != null) {
          try {
            out.close();
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }
      
      boolean moveResult = fs.move(tempPath, realPath);
      if(!moveResult) {
        throw new IOException("Can't move " + tempPath + " to " + realPath);
      }
      LOG.info("move :" + tempPath + "," + realPath);
      LOG.info("BlobMerger end: #files:" + mergeFileInfos.size() + 
          ", mergeTargetFileCount=" + mergeTargetFileCount + 
          ", mergeTargetFileLength=" + mergeTargetFileLength + 
          ", newFileLength=" + fs.getLength(realPath));
    } catch (Exception e) {
      e.printStackTrace();
      if(out != null) {
        try {
          out.close();
        } catch (Exception e1) {
          e.printStackTrace();
        }
        
        try {
          LOG.info("Delete temp file:" + tempPath);
          fs.delete(tempPath);
        } catch (IOException e1) {
          LOG.error(e1.getMessage(), e);
        }
      }
      throw e;
    } 
    
    return realPath.toString();
  }
  
  private void deleteRemovedFiles(MergeFileInfo mergeFileInfo) throws IOException {
    if(mergeFileInfo.removedBlobMetaCells.size() == 0) {
      return;
    }
    
    for(BlobMetaCell eachRemovedCell: mergeFileInfo.removedBlobMetaCells) {
      BlobMetaManager.removeBlobMeta(conf, mergeFileInfo.filePath, eachRemovedCell);
    }
  }
  
  private void modifyBlobMeta(String newFilePath) throws Exception {
    for(MergeFileInfo eachFileInfo: mergeFileInfos) {
      if(eachFileInfo.blobMetaCells.size() == 0) {
        continue;
      }
      
      BlobMetaCell blobMetaCell = eachFileInfo.blobMetaCells.get(0);
      
      for(BlobMetaCell eachBlobMetaCell: eachFileInfo.blobMetaCells) {
        BlobColumnCell blobColumnCell = new BlobColumnCell(newFilePath, eachBlobMetaCell.getOffset(), eachBlobMetaCell.getLength());
        
        BlobMetaManager.saveBlobMetaCell(conf, eachBlobMetaCell, blobColumnCell);
        String rowKeyHash = "" + blobMetaCell.getRowKey().toString().hashCode();
        try {
          getColumnCellLock(eachBlobMetaCell);
          BlobMetaManager.saveBlobColumnCell(conf, eachBlobMetaCell, blobColumnCell);
        } finally {
          LockUtil.delete(zk, LockUtil.getZKPath(conf, "/cloudatafs/" + rowKeyHash), true);
        }
      }          
      
      BlobMetaManager.removeBlobMeta(conf, blobMetaCell.getTableName(), blobMetaCell.getColumnName(), eachFileInfo.filePath);
      
      fs.delete(new GPath(eachFileInfo.filePath));
    }
  }

  private void getColumnCellLock(BlobMetaCell blobMetaCell) throws Exception {
    String lockKey = null;
    String rowKeyHash = "" + blobMetaCell.getRowKey().toString().hashCode();
    try {
      if(!existsParent) {
        if(zk.exists(LockUtil.getZKPath(conf, "/cloudatafs"), false) == null) {
          LockUtil.createNodes(zk, LockUtil.getZKPath(conf, "/cloudatafs"), null, CreateMode.PERSISTENT);
        }
        existsParent = true;
      }
      if(zk.exists(LockUtil.getZKPath(conf, "/cloudatafs/" + rowKeyHash), false) == null) {
        LockUtil.createNodes(zk, LockUtil.getZKPath(conf, "/cloudatafs/" + rowKeyHash), null, CreateMode.PERSISTENT);
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
    
    long startTime = System.currentTimeMillis();
    while(true) {
      lockKey = LockUtil.acquireLock(zk, LockUtil.getZKPath(conf, "/cloudatafs/" + rowKeyHash));
      if(lockKey != null) {
        return;
      } else {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
        }
      }
      long elasp = System.currentTimeMillis() - startTime;
      if(elasp > 30 * 1000) {  //timeout: 30sec
        throw new IOException("Can't get file lock during " + elasp + " ms." + LockUtil.getZKPath(conf, "/cloudatafs/" + rowKeyHash));
      }
    }
  }
  
  public static void main(String args[]) throws IOException {
  }
}
