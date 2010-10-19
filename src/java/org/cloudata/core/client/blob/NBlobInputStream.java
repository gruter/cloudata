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

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.CTableManager;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.fs.CloudataFileSystem;
import org.cloudata.core.fs.GPath;


/**
 * @author jindolk
 *
 */
public class NBlobInputStream extends InputStream {
  private static final Log LOG = LogFactory.getLog(NBlobInputStream.class.getName());
  private InputStream inputStream;
  private long endPos;
  private CloudataFileSystem fs;
  private long currentPos = Long.MIN_VALUE;
  private long offset;
  private long length;
  private String path;
  private long fileLength;
  
  private CTable ctable;
  private Row.Key rowKey;
  private String columnName;
  private Cell.Key cellKey;
  
  public NBlobInputStream(CloudataConf conf, String filePath, BlobMetaCell blobMetaCell) throws IOException {
    this.ctable = CTable.openTable(conf, blobMetaCell.getTableName());
    this.rowKey = blobMetaCell.getRowKey();
    this.columnName = blobMetaCell.getColumnName();
    this.cellKey = blobMetaCell.getCellKey();
    this.fs = CloudataFileSystem.get(ctable.getConf());
    
//    this.path = filePath;
//    this.offset = blobMetaCell.getOffset();
    
    open(); 
  }
  
  public NBlobInputStream(CTable ctable, Row.Key rowKey, String columnName, Cell.Key cellKey) throws IOException {
    this.ctable = ctable;
    this.rowKey = rowKey;
    this.columnName = columnName;
    this.cellKey = cellKey;
    this.fs = CloudataFileSystem.get(ctable.getConf());

    open(); 
  }
  
  public NBlobInputStream(CloudataConf conf, String filePath, long offset, long length) throws IOException {
    //can't reopen
    this.fs = CloudataFileSystem.get(conf);
    
    this.path = filePath;
    this.currentPos = offset;
    this.offset = offset;
    this.length = length;
    this.endPos = offset + length;
    
    GPath nPath = new GPath(path);
    
    //LOG.info("Input Open:" + path);
    this.inputStream = fs.open(nPath);
    
    if(offset > 0) {
      this.inputStream.skip(offset);
    }
    
    this.fileLength = fs.getLength(nPath);
    
    if(fileLength < endPos) {
      throw new IOException("Wrong file info(offset or length): origin file=(" + path + "," + fileLength + "), open params=(offset:" + offset + ",length:" + length + ")");
    }    
  }

  public String getPath() {
    return path;
  }
  
  public long getFileLength() {
    return fileLength;
  }
  
  public long getLength() {
    return length;
  }
  
  public long getOffset() {
    return offset;
  }

  public String toString() {
    return "origin file=(" + path + "," + fileLength + "), open params=(offset:" + offset + ",length:" + length + ")";
  }
  
  @Override
  public int available() throws IOException {
    if(currentPos >= endPos) {
      return 0;
    }
    
    return (int)(endPos - currentPos);
  }

  @Override
  public void close() throws IOException {
    if(inputStream != null) {
      //LOG.info("Input Close:" + path);
      inputStream.close();
    } 
    inputStream = null;
  }

  @Override
  public int read() throws IOException {
    if(currentPos >= endPos) {
      return -1;
    }
    
    int result = 0;
    try {
      result = inputStream.read();
      currentPos++;
    } catch (IOException e) {
      if(!open()) {
        throw e;
      }
      result = inputStream.read();
      currentPos++;
    }
    
    return result;
  }

  private void pause() throws IOException {
    try {
      Thread.sleep(20);
    } catch (InterruptedException e) {
      return;
    }
  }
  
  private boolean open() throws IOException {
    if(rowKey == null) {
      return false;
    }

    boolean noData = false; 
    Exception exception = null;
    long startTime = System.currentTimeMillis();
    while(true) {
      try {
        byte[] fileInfoBytes = ctable.get(rowKey, columnName, cellKey);
        if(fileInfoBytes != null) {
          initOpenInfo(new BlobColumnCell(fileInfoBytes));
          return true;
        } else {
          if(path != null) {
            CTable blobTable = CTable.openTable(ctable.getConf(), 
                BlobMetaManager.getBlobTableName(ctable.getTableName(), columnName));
            byte[] deleted = blobTable.get(new Row.Key(path), Constants.BLOB_COLUMN_DELETE,
                new Cell.Key(BlobMetaManager.OFFSET_DF.format(offset)));
            if(deleted == null) {
              throw new IOException("No data in " + BlobMetaManager.getBlobTableName(ctable.getTableName(), columnName) + "," + 
                  rowKey + "," + columnName + ":" + cellKey);
            }
            if("Y".equals(new String(deleted))) {
              throw new AlreadyDeletedException("deleted " + ctable.getTableName() + "," + rowKey + "," + columnName + ":" + cellKey);
            }
          }
          noData = true;
          break;
        }
      } catch(AlreadyDeletedException e) {
        throw e;
      } catch (Exception e) {
        LOG.debug("get file info: " + e.getMessage());
        exception = e;
      }
      if( (System.currentTimeMillis() - startTime) > ctable.getTxTimeout()) {
        break;
      }
      pause();      
    }
    
    if(exception == null && noData) {
      throw new IOException("No data in " + rowKey + "," + columnName + ":" + cellKey + "," + ctable.getTxTimeout());
    }
    
    if(exception == null) {
      throw new IOException("fail openBlob after " + ctable.getTxTimeout() + " ms");
    }  else {
      throw CTableManager.makeIOException(exception);
    }      
  }
  
  private void initOpenInfo(BlobColumnCell blobColumnCell) throws IOException {
    close();

    this.path = blobColumnCell.getFilePath();
    
    GPath nPath = new GPath(path);
    
    if(!fs.exists(nPath)) {
      throw new IOException("No file:" + path);
    }
    //LOG.info("Input Open:" + path);
    this.inputStream = fs.open(nPath);
    
    //first time
    if(currentPos == Long.MIN_VALUE) {
      this.currentPos = blobColumnCell.getOffset();
      this.offset = blobColumnCell.getOffset();
    }
    
    //reopen
    if(currentPos != offset) {
      long gap = currentPos - offset;
      this.currentPos = blobColumnCell.getOffset() + gap;
      this.inputStream.skip(currentPos);
    } else {
      if(blobColumnCell.getOffset() > 0) {
        this.inputStream.skip(blobColumnCell.getOffset());
      }
      this.currentPos = blobColumnCell.getOffset();
    }
    
    this.path = blobColumnCell.getFilePath();
    this.offset = blobColumnCell.getOffset();
    this.length = blobColumnCell.getLength();
    this.endPos = offset + length;
    
    this.fileLength = fs.getLength(nPath);
    
    if(fileLength < endPos) {
      throw new IOException("Wrong file info(offset or length): origin file=(" + path + "," + fileLength + "), open params=(offset:" + offset + ",length:" + length + ")");
    }
  }
  
  @Override
  public int read(byte[] b, final int off, final int len) throws IOException {
    int localLen = len;
    
    long remain = endPos - currentPos;
    
    if(len > remain) {
      localLen = (int)remain;
    }

    //System.out.println("len:" + len + ",localLen:" + localLen + ",endPos:" + endPos + ",currentPos:" + currentPos);
    int readBytes = 0;
    try {
      readBytes = inputStream.read(b, off, localLen);
    } catch (IOException e) {
      if(!open()) {
        throw e;
      }
      readBytes = inputStream.read(b, off, localLen);
    }
    
    if(readBytes > 0) {
      currentPos += readBytes;
    }
    
    if(readBytes == 0) {
      return -1;
    } else {
      return readBytes;
    }
  }
  
  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }
  
  @Override
  public long skip(long n) throws IOException {
    if(currentPos >= endPos) {
      return -1;
    }
    
    long localLen = n;
    
    long remain = endPos - currentPos;
    
    if(n > remain) {
      localLen = remain;
    }
    
    long skipBytes = 0;
    try {
      skipBytes = inputStream.skip(localLen);
    } catch (IOException e) {
      if(!open()) {
        throw e;
      }
      
      skipBytes = inputStream.skip(localLen);
    }
    
    if(skipBytes > 0) {
      currentPos += skipBytes;
    }
    
    return skipBytes;
  }
  
  public static void main(String[] args) throws IOException {
    if(args.length < 3) {
      System.out.println("Usage: java NBlobInputStream <path> <offset> <length>");
      System.exit(0);
    }
//    NBlobInputStream in = new NBlobInputStream(new NConfiguration(),
//        args[0], Long.parseLong(args[1]), Long.parseLong(args[2]));

    FileSystem fs = FileSystem.get(new Configuration());
    InputStream in = fs.open(new Path(args[0]));
    in.skip(Long.parseLong(args[1]));
    
    byte[] buf = new byte[1024];
    
    long totalRead = 0;
    int readBytes = 0;
    
    while( (readBytes = in.read(buf)) > 0 ) {
      totalRead += readBytes;
      if(readBytes < buf.length) {
        System.out.println("readBytes:" + readBytes + "," + totalRead);
      }
    }
    in.close();
    System.out.println("Read:" + totalRead + " bytes");
  }
  
//  @Override
//  public int read(byte[] b, final int off, final int len) throws IOException {
//    int totalReadNum = 0;
//
//    int localOff = off;
//    int remain = len;
//    
//    while(true) {
//      int readNum = currentInput.read(b, localOff, remain);
//
//      if(readNum < len && !isEnd()) {
//        if(readNum < 0) {
//          readNum = 0;
//        }
//      
//        totalReadNum += readNum;
//        
//        remain -= readNum;
//        localOff += readNum;
//        openNextInputStream();
//      } else {
//        totalReadNum += readNum;
//        break;
//      }
//    } 
//    
//    if(totalReadNum == 0 && isEnd()) {
//      return -1;
//    } else {
//      return totalReadNum;
//    }
//  }
//
//  @Override
//  public long skip(long n) throws IOException {
//    long totalSkipNum = 0;
//
//    long remain = n;
//    
//    while(true) {
//      long skipNum = currentInput.skip(remain);
//
//      if(skipNum < n && !isEnd()) {
//        if(skipNum < 0) {
//          skipNum = 0;
//        }
//      
//        totalSkipNum += skipNum;
//        
//        remain -= skipNum;
//        openNextInputStream();
//      } else {
//        totalSkipNum += skipNum;
//        break;
//      }
//    } 
//    
//    if(totalSkipNum == 0 && isEnd()) {
//      return -1;
//    } else {
//      return totalSkipNum;
//    }
//  }
  
  @Override
  public boolean markSupported() {
    return false;
  }
}
