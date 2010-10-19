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
package org.cloudata.core.tablet;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.exception.TableExistsException;
import org.cloudata.core.common.io.CWritable;
import org.cloudata.core.common.io.CWritableUtils;
import org.cloudata.core.common.lock.LockUtil;


/**
 * Value Object which store Table's schema
 * 
 * @author babokim
 * 
 */
public class TableSchema implements CWritable, Comparable<TableSchema>, Constants {
  //private static final int SCHEMA_VERSION = 1;
  private static final int SCHEMA_VERSION = 2;    //add permission
  
  private static final Log LOG = LogFactory.getLog(TableSchema.class.getName());

  public static final String TABLE_NAME_PATTERN = "[_a-zA-Z][a-zA-Z0-9_]+";
  
  public static final int DEFAULT_NUM_VERSION = 3;

  public static final String DIR_TABLE = "/table/";

  public static final int DEFAULT_TYPE = 0;

  public static final int BLOB_TYPE = 1;

  public static final int CACHE_TYPE = 2;

  private String tableName;

  private String description;

  private List<ColumnInfo> columns;

  private int numOfVersion = DEFAULT_NUM_VERSION;

  private String owner;

  // userId -> w|r
  private Map<String, String> permissions = new HashMap<String, String>();

  private static Set<String> unloadTablesSet = null;
  
  private static Integer mutex = new Integer(0);

  public TableSchema() {
    this("", "");
  }

  public TableSchema(String tableName) {
    this(tableName, "");
  }

  public TableSchema(String tableName, String description) {
    this(tableName, description, new ArrayList<String>());
  }

  public TableSchema(String tableName, String description, List<String> columns) {
    this.tableName = tableName;
    this.description = description;
    this.columns = new ArrayList<ColumnInfo>();

    for (String eachColumn : columns) {
      this.columns.add(new ColumnInfo(eachColumn));
    }
  }

  public TableSchema(String tableName, String description, String[] columns) {
    this.tableName = tableName;
    this.description = description;
    this.columns = new ArrayList<ColumnInfo>();

    for (String eachColumn : columns) {
      this.columns.add(new ColumnInfo(eachColumn));
    }
  }

  public void addColumn(String column) {
    columns.add(new ColumnInfo(column));
  }

  public void addColumn(ColumnInfo columnInfo) {
    columns.add(columnInfo);
  }

  public void removeColumn(ColumnInfo columnInfo) {
    columns.remove(columnInfo);
  }
  
  public void removeColumn(String columnName) {
    columns.remove(new ColumnInfo(columnName));
  }
  
  public List<String> getColumns() {
    List<String> result = new ArrayList<String>();
    for (ColumnInfo eachColumn : columns) {
      result.add(eachColumn.columnName);
    }
    return result;
  }

  public List<ColumnInfo> getColumnInfos() {
    return columns;
  }

  public void setColumns(List<String> columns) {
    this.columns = new ArrayList<ColumnInfo>();

    for (String eachColumn : columns) {
      this.columns.add(new ColumnInfo(eachColumn));
    }
  }

  public void setColumnInfos(List<ColumnInfo> columns) {
    this.columns = columns;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public int getNumOfVersion() {
    return numOfVersion;
  }

  public void setNumOfVersion(int numOfVeriosn) {
    this.numOfVersion = numOfVeriosn;
  }

  public Map<String, String> getPermissions() {
    return permissions;
  }
  
  public int hashCode() {
    return tableName.hashCode();
  }

  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }

    if (!(obj instanceof TableSchema)) {
      return false;
    }

    return tableName.equals(((TableSchema) obj).tableName);
  }

  public static TableSchema loadTableSchema(CloudataConf conf,
      ZooKeeper zk, String tableName) throws IOException {
    String tableLockPath = LockUtil.getZKPath(conf, Constants.PATH_SCHEMA + "/" + tableName);

    byte[] tableData;
    try {
      tableData = zk.getData(tableLockPath, false, null);
    } catch (NoNodeException e) {
      return null;
    } catch (Exception e) {
      throw new IOException(e);
    }
    if(tableData == null) {
      return null;
    }
    
    TableSchema table = new TableSchema();
    table.readFields(new DataInputStream(new ByteArrayInputStream(tableData)));

	/*
    if(unloadTablesSet.contains(table.getTableName())) {
      LOG.debug(table.getTableName() + " is unloadTable");
      return null;
    }
    	*/
    return table;
  }

  public void saveTableSchema(CloudataConf conf, ZooKeeper zk) throws IOException,
      TableExistsException {
    String tablePath = LockUtil.getZKPath(conf, PATH_SCHEMA + "/" + tableName);
    
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    this.write(new DataOutputStream(out));
    try {
      LockUtil.createNodes(zk, tablePath, out.toByteArray(), CreateMode.PERSISTENT);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public String getOwnerInfo() throws IOException {
    StringBuffer sb = new StringBuffer();

    if (owner == null) {
      sb.append("\n");
    } else {
      sb.append(owner).append("\n");
    }

    for (Map.Entry<String, String> entry : permissions.entrySet()) {
      sb.append(entry.getKey()).append("\t").append(entry.getValue()).append(
          "\n");
    }

    return sb.toString();
  }

  public void readFields(DataInput in) throws IOException {
    int version = in.readInt();
    tableName = CWritableUtils.readString(in);
    description = CWritableUtils.readString(in);
    numOfVersion = in.readInt();
    owner = CWritableUtils.readString(in);
    int count = in.readInt();
    
    columns = new ArrayList<ColumnInfo>();
    for (int i = 0; i < count; i++) {
      ColumnInfo columnInfo = new ColumnInfo();
      columnInfo.readFields(in);
      columns.add(columnInfo);
    }
    
    if(version == 1) {    //no permission info
      return;
    }
    
    int permissionLength = in.readInt();
    for(int i = 0; i < permissionLength; i++) {
      String pUserId = CWritableUtils.readString(in);
      String pType = CWritableUtils.readString(in);
      
      permissions.put(pUserId, pType);
    }
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(SCHEMA_VERSION);
    CWritableUtils.writeString(out, tableName);
    CWritableUtils.writeString(out, description);
    out.writeInt(numOfVersion);
    CWritableUtils.writeString(out, owner);
    int size = columns.size();
    out.writeInt(size);
    for (ColumnInfo column : columns) {
      column.write(out);
    }
    
    out.writeInt(permissions.size());
    for(Map.Entry<String, String> entry: permissions.entrySet()) {
      CWritableUtils.writeString(out, entry.getKey());
      CWritableUtils.writeString(out, entry.getValue());
    }
  }

  public String getColumn(int index) {
    return columns.get(index).columnName;
  }
  
  public ColumnInfo getColumnInfo(int index) {
    return columns.get(index);
  }

  public ColumnInfo getColumnInfo(String columnName) {
    for(ColumnInfo column: columns) {
      if(column.columnName.equals(columnName)) {
        return column;
      }
    }
    return null;
  }
  
  public boolean isBlobType(String columnName) throws IOException {
    ColumnInfo columnInfo = getColumnInfo(columnName); 
    if(columnInfo == null) {
      throw new IOException("No column:" + columnName);
    }
    return  columnInfo.getColumnType() == TableSchema.BLOB_TYPE;
  }
  
  public String[] getColumnsArray() {
    String[] columnArray = new String[columns.size()];
    
    for(int i = 0; i < columnArray.length; i++) {
      columnArray[i] = columns.get(i).columnName;
    }
    
    return columnArray;
  }

  public ColumnInfo[] getColumnInfoArray() {
    ColumnInfo[] columnArray = columns.toArray(new ColumnInfo[] {});
    return columnArray;
  }
  
  public boolean containsColumn(String[] targetColumns) {
    if (targetColumns == null || targetColumns.length == 0) {
      return false;
    }

    Set<ColumnInfo> columnSet = new HashSet<ColumnInfo>();
    columnSet.addAll(columns);
    for (String eachColumn : targetColumns) {
      if (!columnSet.contains(eachColumn)) {
        return false;
      }
    }
    return true;
  }

  public String toString() {
    return tableName;
  }

  public void print() {
    System.out.println("Table name: " + tableName);
    System.out.println("Desc: " + description);
    System.out.println("Columns:");
    for (ColumnInfo column : columns) {
      System.out.println("\t" + column.columnName + "," + column.columnType);
    }
  }

  public static String getTableDataPath(CloudataConf conf, String tableName) {
    return conf.get("cloudata.root") + DIR_TABLE + tableName;
  }

  public static String getTableDataTrashPath(CloudataConf conf,
      String tableName) {
    return conf.get("cloudata.root") + "/trash" + DIR_TABLE
        + System.currentTimeMillis() + "/" + tableName;
  }

  public int compareTo(TableSchema o) {
    return tableName.compareTo(o.tableName);
  }

  public String getTableDetail() {
    return "owner=" + owner + "; # versions=" + numOfVersion;
  }

  public String getOwner() {
    return owner;
  }

  public void setOwner(String owner) {
    this.owner = owner;
  }

  public boolean hasPermission(String userId, String readWrite) {
    // LOG.fatal("Owner:" + owner + "," + userId);
    if (userId.equals(owner)) {
      return true;
    }

    String permission = permissions.get("*");
    if (permission == null) {
      permission = permissions.get(userId);
    }

    if (permission == null) {
      return false;
    }

    if ("w".equals(permission)) {
      return true;
    } else {
      // 권한 설정은 read이고 request는 write 요청이 온 경우
      if ("w".equals(readWrite)) {
        return false;
      } else {
        return true;
      }
    }
  }

  public void addPermission(String userId, String permission) {
    permissions.put(userId, permission);
  }

  public void removePermission(String userId) {
    permissions.remove(userId);
  }

  public static class ColumnInfo implements CWritable {
    private String columnName;

    private int numOfVersion = DEFAULT_NUM_VERSION;

    private int columnType = DEFAULT_TYPE;

    public ColumnInfo() {
    }

    public ColumnInfo(String columnName) {
      this(columnName, DEFAULT_TYPE);
    }

    public ColumnInfo(String columnName, int columnType) {
      this.columnName = columnName;
      this.columnType = columnType;
    }

    protected String getString() {
      return Constants.MAJOR_VERSION + "\t" + columnName + "\t" + columnType + "\t" + numOfVersion;
    }
    
    protected void parseFromString(String str) throws IOException {
      String[] tokens = str.split("\t");
      if(tokens.length < 4) {
        throw new IOException("Wrong column format: " + str);
      }
      
      this.columnName = tokens[1];
      this.columnType = Integer.parseInt(tokens[2]);
      this.numOfVersion = Integer.parseInt(tokens[3]);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
      columnName = CWritableUtils.readString(in);
      columnType = in.readInt();
      numOfVersion = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      CWritableUtils.writeString(out, columnName);
      out.writeInt(columnType);
      out.writeInt(numOfVersion);
    }

    public String getColumnName() {
      return columnName;
    }

    public int getNumOfVersion() {
      return numOfVersion;
    }

    public int getColumnType() {
      return columnType;
    }
    
    public boolean equals(Object o) {
      ColumnInfo c = (ColumnInfo)o;
      
      return columnName.equals(c.columnName);
    }
    
    public String toString() {
      if(columnType == DEFAULT_TYPE) {
        return columnName;
      } else if(columnType == CACHE_TYPE) {
        return columnName + "(CACHE)";
      } else {
        return columnName + "(BLOB)";
      }
    }
  }
}
