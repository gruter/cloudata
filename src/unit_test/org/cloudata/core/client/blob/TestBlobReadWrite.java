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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CellFilter;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.RowFilter;
import org.cloudata.core.client.blob.BlobColumnCell;
import org.cloudata.core.client.blob.BlobManager;
import org.cloudata.core.client.blob.BlobMetaManager;
import org.cloudata.core.client.blob.NBlobInputStream;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.tablet.TableSchema;
import org.cloudata.core.tablet.TableSchema.ColumnInfo;


/**
 * BLOB 처리 regression 테스트
 * @author jindolk
 *
 */
public class TestBlobReadWrite {
  private static final Log LOG = LogFactory.getLog(TestBlobReadWrite.class.getName());
  
  static CloudataConf conf = new CloudataConf();
  
  static BlobManager blobManager;
  
  static final String NORMAL_COLUMN = "LOCAL_PATH";
  static final String BLOB_COLUMN = "UPLOADED_FILE";
  
  static FileWriteThread writeThread1;
  static FileWriteThread writeThread2;
  
  static FileReadThread readThread1;
  static FileReadThread readThread2;
  
  static FileRemoveThread removeThread;
  
  static Set<String> removedDatas = new HashSet<String>();
  
  static boolean stop = false;
  
  private static void printBlobTableData() throws IOException {
    CTable ctable = CTable.openTable(conf, BlobMetaManager.getBlobTableName("T_BLOB_TEST01", BLOB_COLUMN));
    
    RowFilter rowFilter = new RowFilter(Row.Key.MIN_KEY, Row.Key.MAX_KEY, RowFilter.OP_BETWEEN);
    rowFilter.addCellFilter(new CellFilter(Constants.BLOB_COLUMN_FILE));
    rowFilter.addCellFilter(new CellFilter(Constants.BLOB_COLUMN_DELETE));
    rowFilter.setPagingInfo(10, null);
    
    while(true) {
      Row[] rows = ctable.gets(rowFilter);
      if(rows == null || rows.length == 0) {
        break;
      }
      for(Row eachRow: rows) {
        Map<Cell.Key, Cell> cells = eachRow.getCellMap(Constants.BLOB_COLUMN_DELETE);
        if(cells == null || cells.size() == 0) {
          LOG.warn("No " +  Constants.BLOB_COLUMN_DELETE + " datas in row:" + eachRow.getKey());
        }

        List<Cell> columnFileCells = eachRow.getCellList(Constants.BLOB_COLUMN_FILE);
        if(columnFileCells == null || columnFileCells.size() == 0) {
          LOG.warn("No " + Constants.BLOB_COLUMN_FILE + " datas in row:" + eachRow.getKey());
        } else {
          for(Cell eachCell: columnFileCells) {
            System.out.println(eachRow.getKey() + "," + eachCell.getKey() + "," + (new BlobColumnCell(eachCell.getBytes())).toString());
          }
        }
      }
      rowFilter.setPagingInfo(10, rows[rows.length - 1].getKey());
    }
  }
  
  private static void printTableData() throws IOException {
    CTable ctable = CTable.openTable(conf, "T_BLOB_TEST01");
    
    RowFilter rowFilter = new RowFilter(Row.Key.MIN_KEY, Row.Key.MAX_KEY, RowFilter.OP_BETWEEN);
    rowFilter.addCellFilter(new CellFilter(NORMAL_COLUMN));
    rowFilter.addCellFilter(new CellFilter(BLOB_COLUMN));
    rowFilter.setPagingInfo(10, null);
    
    while(true) {
      Row[] rows = ctable.gets(rowFilter);
      if(rows == null || rows.length == 0) {
        break;
      }
      for(Row eachRow: rows) {
        Map<Cell.Key, Cell> cells = eachRow.getCellMap(BLOB_COLUMN);
        if(cells == null || cells.size() == 0) {
          LOG.warn("No " +  BLOB_COLUMN + " datas in row:" + eachRow.getKey());
        }

        List<Cell> localPathCells = eachRow.getCellList(NORMAL_COLUMN);
        if(localPathCells == null || localPathCells.size() == 0) {
          LOG.warn("No " + NORMAL_COLUMN + " datas in row:" + eachRow.getKey());
        } else {
          for(Cell eachCell: localPathCells) {
            //ColumnBlobInfo blobInfo = new ColumnBlobInfo(eachCell.getBytes());
            //System.out.println(eachCell.getKey() + "," + blobInfo.toString());
            System.out.println(eachRow.getKey() + "," + eachCell.getKey() + "," + new String(eachCell.getBytes()));
            Cell fileCell = cells.get(eachCell.getKey());
            BlobColumnCell blobInfo = new BlobColumnCell(fileCell.getBytes());
            System.out.println(">>>>" + blobInfo.toString());
          }
        }
      }
      rowFilter.setPagingInfo(10, rows[rows.length - 1].getKey());
    }
  }
  
  private static void shutdown() {
    //blobManager.shutdown();
    
    stop = true;
    
    if(writeThread1 != null) {
      writeThread1.interrupt();
    }
    
    if(writeThread2 != null) {
      writeThread2.interrupt();
    }
    
    if(readThread1 != null) {
      readThread1.interrupt();
    }
    
    if(readThread2 != null) {
      readThread2.interrupt();
    }
    
    if(removeThread != null) {
      removeThread.interrupt();
    }
    
    System.exit(0);
  }
  
  public static void main(String[] args) throws IOException {
    if(args.length < 2) {
      printBlobTableData();
      System.out.println("Usage: java TestNTableBlobReadWrite <local dir> <table name>");
      System.exit(0);
    }
    if(CTable.existsTable(conf, args[1])) {
      CTable.dropTable(conf, args[1]);
    }
    
    TableSchema tableSchema = new TableSchema(args[1]);
    tableSchema.addColumn(new ColumnInfo(NORMAL_COLUMN));
    tableSchema.addColumn(new ColumnInfo(BLOB_COLUMN, TableSchema.BLOB_TYPE));
    CTable.createTable(conf, tableSchema);
    
    writeThread1 = new FileWriteThread("A1", args[0], args[1]);
    writeThread1.start();
    
    writeThread2 = new FileWriteThread("A2", args[0], args[1]);
    writeThread2.start();
    
    readThread1 = new FileReadThread(args[1]);
    readThread1.start();
    
    readThread2 = new FileReadThread(args[1]);
    readThread2.start();
    
    removeThread = new FileRemoveThread(args[1]);
    removeThread.start();
  }
  
  static class FileWriteThread extends Thread {
    private String threadId;
    private String localPath;
    private CTable ctable;
    
    public FileWriteThread(String threadId, String localPath, String tableName) throws IOException {
      this.threadId = threadId;
      this.localPath = localPath;
      
      ctable = CTable.openTable(conf, tableName);
    }
    
    public void run() {
      try {
        File file = new File(localPath);
        File[] files = file.listFiles();
        
        if(files == null || files.length == 0) {
          System.out.println("No files in " + localPath);
          shutdown();
        }
        
        DecimalFormat df = new DecimalFormat("0000000000");
        long seqNum = 1;
        while(!stop) {
          int cellNum = 1;
          for(File eachFile: files) {
            if(eachFile.isFile()) {
              uploadFile(eachFile, df.format(seqNum), df.format(cellNum));
              if(seqNum % 100 == 0) {
                LOG.info(threadId + " " + seqNum + " file uploaded");
              }
              cellNum++;
              seqNum++;
              
              if(seqNum % 10 == 0) {
                Thread.sleep(2 * 1000);
              }
            }
          }
        }
      } catch (InterruptedException e) {
        return;
      } catch (Exception e) {
        e.printStackTrace();
        shutdown();
      }
    }
    
    private void uploadFile(File file, String rowNum, String cellNum) throws IOException {
      Row.Key rowKey = new Row.Key(threadId + rowNum);
      Cell.Key cellKey = new Cell.Key(cellNum);
      
      Row row = new Row(rowKey);
      row.addCell(NORMAL_COLUMN, new Cell(cellKey, file.getPath().getBytes()));
      
      ctable.put(row);
      
      BufferedOutputStream out = new BufferedOutputStream(ctable.createBlob(rowKey, BLOB_COLUMN, cellKey));
      
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
  
  static class FileReadThread extends Thread {
    private CTable ctable;
    
    public FileReadThread(String tableName) throws IOException {
      super("FileReadThread");
      ctable = CTable.openTable(conf, tableName);
    }
    
    public void run() {
      try {
        while(!stop) {
          LOG.info("file compared start");
          RowFilter rowFilter = new RowFilter(Row.Key.MIN_KEY, Row.Key.MAX_KEY, RowFilter.OP_BETWEEN);
          rowFilter.addCellFilter(new CellFilter(NORMAL_COLUMN));
          rowFilter.addCellFilter(new CellFilter(BLOB_COLUMN));
          rowFilter.setPagingInfo(10, null);
          
          int count = 0;
          while(true) {
            Row[] rows = ctable.gets(rowFilter);
            
            if(rows == null || rows.length == 0) {
              break;
            }
            for(Row eachRow: rows) {
              Map<Cell.Key, Cell> cells = eachRow.getCellMap(BLOB_COLUMN);
              if(cells == null || cells.size() == 0) {
                //Stream에 close 되기전에는 BLOB_COLUMN 정보는 저장되지 않는다.
                //LOG.info("No " + BLOB_COLUMN + " datas in row:" + eachRow.getKey());
                continue;
              }

              List<Cell> localPathCells = eachRow.getCellList(NORMAL_COLUMN);
              if(localPathCells == null || localPathCells.size() == 0) {
                //LOG.info("No " + NORMAL_COLUMN + " datas in row:" + eachRow.getKey());
                continue;
              }
              
              for(Cell eachCell: localPathCells) {
                if(!cells.containsKey(eachCell.getKey())) {
                  continue;
                }
                
                String localPath = eachCell.getValueAsString();
                
                NBlobInputStream blobIn = null;
                try {
                  blobIn = (NBlobInputStream)ctable.openBlob(eachRow.getKey(), BLOB_COLUMN, eachCell.getKey());
                  compareFile(localPath, blobIn);
//                  synchronized(removedDatas) {
//                    if(removedDatas.contains(eachRow.getKey().toString()) ||
//                        removedDatas.contains(eachRow.getKey().toString() + eachCell.getKey().toString())) {
//                      System.out.println("FAIL: Can access removed data");
//                      shutdown();
//                      return;
//                    }
//                  }
                  count++;
                } catch (Exception e) {
                  synchronized(removedDatas) {
                    if(removedDatas.contains(eachRow.getKey().toString()) ||
                        removedDatas.contains(eachRow.getKey().toString() + eachCell.getKey().toString())) {
                      continue;
                    }
                  }
                  LOG.error(localPath + "," + eachRow.getKey() + "," + eachCell.getKey() + ":" + e.getMessage(), e);
                  shutdown();
                  return;
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
        }
      } catch (InterruptedException e) {
        return;
      } catch (Exception e) {
        e.printStackTrace();
        shutdown();
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
        
//        while( (blobRead = blobStream.read(blobBuf)) > 0) {
//          blobTotalRead += blobRead;
//        }
//        
//        if(blobTotalRead != file.length()) {
//          throw new IOException("Different read bytes: " + blobTotalRead + "," + file.length());
//        }
        
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
  
  static class FileRemoveThread extends Thread {
    private CTable ctable;
    
    public FileRemoveThread(String tableName) throws IOException {
      super("FileRemoveThread");
      ctable = CTable.openTable(conf, tableName);
    }
    
    public void run() {
      LOG.info("FileRemoveThread started");
      try {
        while(!stop) {
          RowFilter rowFilter = new RowFilter(Row.Key.MIN_KEY, Row.Key.MAX_KEY, RowFilter.OP_BETWEEN);
          rowFilter.addCellFilter(new CellFilter(NORMAL_COLUMN));
          rowFilter.addCellFilter(new CellFilter(BLOB_COLUMN));
          rowFilter.setPagingInfo(10, null);
          
          int removeRowCount = 0;
          int removeCellCount = 0;
          int count = 0;
          int rowCount = 0;
          while(!stop) {
            Row[] rows = ctable.gets(rowFilter);
            
            if(rows == null || rows.length == 0) {
              break;
            }
            
            for(Row eachRow: rows) {
              if(rowCount++ % 50 == 0) {
                synchronized(removedDatas) {
                  removedDatas.add(eachRow.getKey().toString());
                }
                ctable.removeRow(eachRow.getKey());
                //LOG.info("Remove row:" + eachRow.getKey());
                removeRowCount++;
              } else {
                Map<Cell.Key, Cell> cells = eachRow.getCellMap(BLOB_COLUMN);
                if(cells == null || cells.size() == 0) {
                  //LOG.warn("No " + BLOB_COLUMN + " data:" + eachRow.getKey());
                  continue;
                }
                List<Cell> localPaths = eachRow.getCellList(NORMAL_COLUMN);
                if(localPaths == null || localPaths.size() == 0) {
                  //LOG.warn("No " + NORMAL_COLUMN + " data:" + eachRow.getKey());
                  continue;
                }
                for(Cell eachCell: localPaths) {
                  if(!cells.containsKey(eachCell.getKey())) {
                    continue;
                  }
                  count ++;
                  
                  if(count % 10 == 0) {
                    synchronized(removedDatas) {
                      removedDatas.add(eachRow.getKey().toString() + eachCell.getKey().toString());
                    }
                    ctable.remove(eachRow.getKey(), BLOB_COLUMN, eachCell.getKey());
                    removeCellCount++;
                    //LOG.info("Remove cell:" + eachRow.getKey() + "," + eachCell.getKey());
                  }
                }
              }
            }
            
            rowFilter.setPagingInfo(10, rows[rows.length - 1].getKey());
          }
          
          if(removeRowCount > 0 || removeCellCount > 0) {
            LOG.info(removeRowCount + " rows removed. " + removeCellCount + " cells removed");
          }
          Thread.sleep(30 * 1000);
        }
      } catch (InterruptedException e) {
        return;
      } catch (Exception e) {
        e.printStackTrace();
        shutdown();
      }
    }
  }
}
