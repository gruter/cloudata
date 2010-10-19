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
package org.cloudata.tools.cloudatafs;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CellFilter;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.RowFilter;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.lock.LockUtil;
import org.cloudata.core.common.util.StringUtils;
import org.cloudata.core.tablet.TableSchema;
import org.cloudata.core.tablet.TableSchema.ColumnInfo;


/**
 * @author jindolk
 *
 */
public class CloudataFS {
  public static int VERSION = 1;
  
  public static final Log LOG = LogFactory.getLog(CloudataFS.class.getName());
  private static ReentrantReadWriteLock fileSystemsLock = new ReentrantReadWriteLock();
  private static CloudataFS instance;
  private static final String FS_TABLE_NAME = "_CloudFS";
  protected static final String INODE_COLUMN = "INODE";
  protected static final String FILE_COLUMN = "FILE";
  
  protected static final DecimalFormat df = new DecimalFormat("00000000");
  
  private CloudataConf conf;
  private CTable ctable;

  private ZooKeeper zk;
  /**
   * get NetuneFS instance
   * @param conf
   * @return 
   * @throws IOException
   */
  public static CloudataFS get(CloudataConf conf) throws IOException {
    fileSystemsLock.writeLock().lock();
    try {
      String metaTableName = conf.get("CloudataFs.tableName", FS_TABLE_NAME);
      creatMetaTableIfNotExists(conf, metaTableName);

      instance = new CloudataFS(conf);
    } finally {
      fileSystemsLock.writeLock().unlock();
    }
    return instance;
  }

  private static void creatMetaTableIfNotExists(CloudataConf conf, String metaTableName) throws IOException {
    if(CTable.existsTable(conf, metaTableName)) {
      return;
    }
    TableSchema tableSchema = new TableSchema(metaTableName);
    tableSchema.addColumn(new ColumnInfo(INODE_COLUMN));
    tableSchema.addColumn(new ColumnInfo(FILE_COLUMN, TableSchema.BLOB_TYPE));
    tableSchema.addPermission("*", "r");
    tableSchema.addPermission("*", "w");
    
    CTable.createTable(conf, tableSchema);
  }
  
  private CloudataFS(CloudataConf conf) throws IOException {
    this.conf = conf;
    String metaTableName = conf.get("CloudataFs.tableName", FS_TABLE_NAME);

    if(!CTable.existsTable(conf, metaTableName)) {
      throw new IOException("No meta table [" + metaTableName + "]");
    }
    this.ctable = CTable.openTable(conf, metaTableName);

    if(!exists("/")) {
      mkdir("/");
    }
    
    zk = LockUtil.getZooKeeper(conf, "CloudataFS", null);
  }
  
  /**
   * 
   * @param path
   * @return
   * @throws IOException
   */
  public String[] listPath(String path) throws IOException {
    RowFilter rowFilter = getRowFilterForListing(path);

    List<String> paths = new ArrayList<String>();
    Iterator<Row> iterator = ctable.iterator(rowFilter);
    
    while(iterator.hasNext()) {
      Row row = iterator.next();
      
      Cell cell = row.getFirst(INODE_COLUMN);
      if(cell == null) {
        break;        
      }
      
      FileNode fileNode = new FileNode(cell.getBytes());
      
      paths.add(fileNode.getPath());
    }
    
    if(paths.size() == 0) {
      return null;
    } else {
      return paths.toArray(new String[]{});
    }
  }

  /**
   * @param path
   * @return
   * @throws IOException
   */
  private RowFilter getRowFilterForListing(String path) throws IOException {
    path = normalizePath(path);

    String[] tokens = path.split("/");
    
    Row.Key startRowKey;
    Row.Key endRowKey;
    
    if("/".equals(path)) {
      startRowKey = new Row.Key("/" + df.format(1) + "_"); 
      endRowKey = new Row.Key("/" + df.format(2) + "_"); 
    } else {
      String tail = path.substring(1);
      startRowKey = new Row.Key("/" + df.format(tokens.length) + "_" + tail); 
      
      int length = startRowKey.getLength();
      byte[] maxKeyBytes = new byte[length + 1];
      System.arraycopy(startRowKey.getBytes(), 0, maxKeyBytes, 0, length);
      maxKeyBytes[maxKeyBytes.length - 1] = (byte)0xFF;

      endRowKey = new Row.Key(maxKeyBytes); 
    }
    
    RowFilter rowFilter = new RowFilter(startRowKey, endRowKey, RowFilter.OP_BETWEEN);
    rowFilter.addCellFilter(new CellFilter(INODE_COLUMN));
    rowFilter.setPagingInfo(100, null);
    return rowFilter;
  }
  
  /**
   * 
   * @param path
   * @return
   * @throws IOException
   */
  public Iterator<FileNode> iterator(String path) throws IOException {
    RowFilter rowFilter = getRowFilterForListing(path);

    Iterator<Row> iterator = ctable.iterator(rowFilter);

    return new FileNodeIterator(iterator);
  }
  
  public FileNode[] listFileNode(String path) throws IOException {
    RowFilter rowFilter = getRowFilterForListing(path);

    List<FileNode> nodes = new ArrayList<FileNode>();
    Iterator<Row> iterator = ctable.iterator(rowFilter);
    
    while(iterator.hasNext()) {
      Row row = iterator.next();
      
      Cell cell = row.getFirst(INODE_COLUMN);
      if(cell == null) {
        break;        
      }
      
      FileNode fileNode = new FileNode(cell.getBytes());
      
      nodes.add(fileNode);
    }
    
    if(nodes.size() == 0) {
      return null;
    } else {
      return nodes.toArray(new FileNode[]{});
    }
  }
  
  private void mkdir(String path, String user, boolean checkExists) throws IOException {
    if(exists(path)) {
      if(checkExists) {
        throw new IOException("Already exists dir:" + path);
      } else {
        return;
      }
    }
    path = normalizePath(path);
    String npath = makeRowKeyPath(path);
    Row.Key rowKey = new Row.Key(npath);
    
    FileNode fileNode = new FileNode();
    fileNode.setPath(path);
    fileNode.setVersion(CloudataFS.VERSION);
    fileNode.setType(FileNode.DIR);
    fileNode.setUser(user);
    
    Row row = new Row(rowKey);
    row.addCell(INODE_COLUMN, new Cell(Cell.Key.EMPTY_KEY, fileNode.getBytes()));
    ctable.put(row);
  }
  
  public void mkdir(String path) throws IOException {
    mkdir(path, null, true);
  }
  
  public void mkdirs(String path, boolean checkExists) throws IOException {
    String[] tokens = path.split("/");
    
    String currentPath = "/";
    for(int i = 0; i < tokens.length - 1; i++) {
      currentPath += tokens[i];
      if(!exists(currentPath)) {
        mkdir(currentPath, null, checkExists);
      }
      if(tokens[i].length() > 0) {
        currentPath += "/";
      }
    }
    
    mkdir(path, null, checkExists);
  }
  
  public void mkdirs(String path) throws IOException {
    mkdirs(path, true);
  }
  
  public InputStream open(String path) throws IOException {
    if(!exists(path)) {
      throw new IOException("No file:" + path);
    }
    path = normalizePath(path);
    String npath = makeRowKeyPath(path);
    Row.Key rowKey = new Row.Key(npath);

    return ctable.openBlob(rowKey, FILE_COLUMN, Cell.Key.EMPTY_KEY);
  }

  public OutputStream create(String path) throws IOException {
    path = normalizePath(path);
    if("/".equals(path)) {
      throw new IOException("Can't makr file with root path(/)");
    }
    if(exists(path)) {
      throw new IOException("Already exists:" + path);
    }
    
    String parentPath = getParentPath(path);
    if(!exists(parentPath)) {
      throw new IOException("Not exists parent dir:" + parentPath);
    }
    
    String npath = makeRowKeyPath(path);
    Row.Key rowKey = new Row.Key(npath);
    
    FileNode fileNode = new FileNode();
    fileNode.setPath(path);
    fileNode.setVersion(CloudataFS.VERSION);
    fileNode.setType(FileNode.FILE);
    //fileNode.setUser(owner);

    OutputStream out = new CloudataFSOutputStream(ctable, fileNode);

    Row row = new Row(rowKey);
    row.addCell(INODE_COLUMN, new Cell(Cell.Key.EMPTY_KEY, fileNode.getBytes()));
    ctable.put(row);
    
    return out;
  }
  
  private boolean deleteInternal(String path) throws IOException {
    path = normalizePath(path);
    String npath = makeRowKeyPath(path);
    ctable.removeRow(new Row.Key(npath));
    return true;
  }
  
  public boolean delete(String path) throws IOException {
    return delete(path, false);
  }
  
  public boolean delete(String path, boolean recursive) throws IOException {
    if (!exists(path)) {
      return true;
    }
    if (isDir(path)) {
      FileNode[] srcs = listFileNode(path);
      int length = (srcs != null ? srcs.length : 0);
      if(length > 0 && !recursive) {
        throw new IOException("Delete error [" + path + " is not empty]");
      }
      for (int i = 0; i < length; i++) {
        if (recursive) {
          if (srcs[i].isDir()) {
            if (!delete(srcs[i].getPath(), recursive)) {
              return false;
            }
          } else {
            if (!deleteInternal(srcs[i].getPath())) {
              return false;
            }
          }
        } else {
          if (!deleteInternal(srcs[i].getPath())) {
            return false;
          }
        }
      }
    }
    if (!deleteInternal(path)) {
      return false;
    }

    return true;
  }
  
  public boolean exists(String path) throws IOException {
    path = normalizePath(path);
    return getFileNode(path) != null;
  }
  
  public boolean isDir(String path) throws IOException {
    path = normalizePath(path);
    FileNode fileNode = getFileNode(path);
    if(fileNode == null) {
      throw new IOException("No file:" + path);
    }
    
    return fileNode.getType() == FileNode.DIR;
  }
  
  /**
   * get file length
   * @param path
   * @return
   * @throws IOException
   */
  public long getLength(String path) throws IOException {
    path = normalizePath(path);
    FileNode fileNode = getFileNode(path);
    if(fileNode == null) {
      throw new IOException("No file:" + path);
    }
    
    return fileNode.getLength();
  }
  
  
  /**
   * move file or dir
   * @param srcPath
   * @param destPath
   * @return
   * @throws IOException
   */
  public boolean move(String srcPath, String destPath) throws IOException {
    srcPath = normalizePath(srcPath);
    destPath = normalizePath(destPath);

    if("/".equals(srcPath) || "/".equals(destPath)) {
      return false;
    }
    
    FileNode[] fileNodes = listFileNode(srcPath);
    if(fileNodes == null || fileNodes.length == 0) {
      throw new IOException("No file or dir: " +  srcPath);
    }
    
    mkdirs(destPath, false);
    
    List<String> oldDirs = new ArrayList<String>();
    
    String parentPath = destPath;
    for(FileNode eachFileNode: fileNodes) {
      if(eachFileNode.isDir()) {
        oldDirs.addAll(moveInternal(eachFileNode.getPath(), parentPath + "/" + eachFileNode.getPathName()));
      } else {
        rename(eachFileNode.getPath(), parentPath + "/" + eachFileNode.getPathName());
      }
    }
    
    oldDirs.add(srcPath);

    for(String eachOldDir: oldDirs) {
      delete(eachOldDir, true);
    }
    
    return true;
  }
  
  private List<String> moveInternal(String srcPath, String destPath) throws IOException {
    mkdirs(destPath, false);
    
    List<String> oldDirs = new ArrayList<String>();
    
    FileNode[] fileNodes = listFileNode(srcPath);
    if(fileNodes == null || fileNodes.length == 0) {
      return oldDirs;
    }
    
    String parentPath = destPath;
    for(FileNode eachFileNode: fileNodes) {
      if(eachFileNode.isDir()) {
        oldDirs.addAll(moveInternal(eachFileNode.getPath(), parentPath + "/" + eachFileNode.getPathName()));
      } else {
        rename(eachFileNode.getPath(), parentPath + "/" + eachFileNode.getPathName());
      }
    }
    
    return oldDirs;
  }

  private boolean rename(String srcPath, String destPath) throws IOException {
    String srcRowKeyPath = makeRowKeyPath(srcPath);
    String destRowKeyPath = makeRowKeyPath(destPath);
    
    Row.Key rowKey = new Row.Key(srcRowKeyPath);
    Row row = ctable.get(rowKey);
    
    if(row == null) {
      return false;
    }
    
    Cell iNodeCell = row.getFirst(INODE_COLUMN);
    if(iNodeCell == null) {
      return false;
    }

    Cell fileCell = row.getFirst(FILE_COLUMN);
    if(fileCell == null) {
      return false;
    }

    FileNode fileNode = new FileNode(iNodeCell.getBytes());
    fileNode.setPath(destPath);
    
    String lockKey = null;
    String destRowKeyPathHash = "" + destRowKeyPath.hashCode();
    try {
      try {
        if(zk.exists(LockUtil.getZKPath(conf, "/cloudatafs"), false) == null) {
          LockUtil.createNodes(zk, LockUtil.getZKPath(conf, "/cloudatafs"), null, CreateMode.PERSISTENT);
        }
        if(zk.exists(LockUtil.getZKPath(conf, "/cloudatafs/" + destRowKeyPathHash), false) == null) {
          LockUtil.createNodes(zk, LockUtil.getZKPath(conf, "/cloudatafs/" + destRowKeyPathHash), null, CreateMode.PERSISTENT);
        }
      } catch (Exception e) {
        throw new IOException(e);
      }
      
      long startTime = System.currentTimeMillis();
      while(true) {
        lockKey = LockUtil.acquireLock(zk, LockUtil.getZKPath(conf, "/cloudatafs/" + destRowKeyPathHash));
        if(lockKey != null) {
          break;
        } else {
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
          }
        }
        long elasp = System.currentTimeMillis() - startTime;
        if(elasp > 30 * 1000) {  //timeout: 30sec
          throw new IOException("Can't get file lock during " + elasp + " ms." + LockUtil.getZKPath(conf, "/cloudatafs/" + destRowKeyPathHash));
        }
      }
      Row renamedRow = new Row(new Row.Key(destRowKeyPath));
      renamedRow.addCell(CloudataFS.INODE_COLUMN, new Cell(Cell.Key.EMPTY_KEY, fileNode.getBytes()));
      renamedRow.addCell(CloudataFS.FILE_COLUMN, fileCell);
      
      ctable.put(renamedRow);
  
      //delete previous file
      ctable.removeRow(rowKey);
      return true;
    } finally {
      LockUtil.delete(zk, LockUtil.getZKPath(conf, "/cloudatafs/" + destRowKeyPathHash), true);
    }
  }
  
  public FileNode getFileNode(String path) throws IOException {
    path = normalizePath(path);
    String npath = makeRowKeyPath(path);
    Row.Key rowKey = new Row.Key(npath);
    Row row = ctable.get(rowKey, INODE_COLUMN);
    
    if(row == null) {
      return null;
    }
    
    Cell cell = row.getFirst(INODE_COLUMN);
    if(cell == null) {
      return null;
    }
    
    return new FileNode(cell.getBytes());
  }
  
  public static String normalizePath(final String path) throws IOException {
    if(path == null) {
      return null;
    }
    
    if(!path.startsWith("/")) {
      throw new IOException("Path must starts with [/]: " + path);
    }
    
    if(path.length() > 1 && path.endsWith("/")) {
      return StringUtils.substring(path, 0, path.getBytes().length - 1);
    } else {
      return path;
    }
  }
  
  protected static String makeRowKeyPath(final String path) throws IOException {
    String[] tokens = path.split("/");
    
    String normalizedPath = "/";
    
    for(int i = 1; i < tokens.length; i++) {
      if(i == 1) {
        normalizedPath += df.format(tokens.length - 1) + "_"; 
      }
      normalizedPath += tokens[i];
      if(i < tokens.length - 1) {
        normalizedPath += "/";
      }
    }
    return normalizedPath;
  }
  
  public static String getPathName(String path) {
    if(path == null) {
      return null;
    }
    
    if("/".equals(path)) {
      return "";
    }
    
    if(path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
    }
    
    int index = path.lastIndexOf("/");
    if(index >= 0) {
      return path.substring(index + 1);
    } else {
      return path;
    }
  }
  
  public static String getParentPath(String path) {
    if(path == null) {
      return null;
    }
    if("/".equals(path)) {
      return "/";
    }
    
    if(path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
    }
    
    int index = path.lastIndexOf("/");
    if(index >= 0) {
      return path.substring(0, index + 1);
    } else {
      return path;
    }
  }
  
  public static void main(String[] args) throws Exception {
  }
}
