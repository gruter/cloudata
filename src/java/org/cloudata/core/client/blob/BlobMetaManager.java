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
import java.text.DecimalFormat;
import java.util.Calendar;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.conf.CloudataConf;


/**
 * @author jindolk
 *
 */
public class BlobMetaManager {
  public static final Log LOG = LogFactory.getLog(BlobMetaManager.class.getName());
  
  public static final DecimalFormat OFFSET_DF = new DecimalFormat("00000000000000000000");
  
  /**
   * Store Blob file info to user table's blob column
   * @param blobMetaCell
   * @param blobColumnCell
   * @throws IOException
   */
  public static void saveBlobColumnCell(CloudataConf conf, BlobMetaCell blobMetaCell, BlobColumnCell blobColumnCell) throws IOException {
    CTable originTable = CTable.openTable(conf, blobMetaCell.getTableName());
    Row originRow = new Row(blobMetaCell.getRowKey());
    originRow.addCell(blobMetaCell.getColumnName(), 
        new Cell(blobMetaCell.getCellKey(), blobColumnCell.getBytes()));
    originTable.put(originRow);
  }

  /**
   * Store user table's column data info to blob table
   * @param blobMetaCell
   * @param blobColumnCell
   * @throws IOException
   */
  public static void saveBlobMetaCell(CloudataConf conf, BlobMetaCell blobMetaCell, BlobColumnCell blobColumnCell) throws IOException {
    CTable blobTable = CTable.openTable(conf, getBlobTableName(blobMetaCell.getTableName(), blobMetaCell.getColumnName()));
    
    Row row = new Row(new Row.Key(blobColumnCell.getFilePath()));
    Cell.Key cellKey = new Cell.Key(OFFSET_DF.format(blobMetaCell.getOffset()));
    row.addCell(Constants.BLOB_COLUMN_FILE, new Cell(cellKey, blobMetaCell.getBytes()));
    row.addCell(Constants.BLOB_COLUMN_DELETE, new Cell(cellKey, "N".getBytes()));
    
    blobTable.put(row);
  }

  public static void removeBlobMeta(CloudataConf conf, String tableName, String columnName, String path) throws IOException {
    //LOG.info("Remove2: " + path);
    CTable blobTable = CTable.openTable(conf, getBlobTableName(tableName, columnName));
    blobTable.removeRow(new Row.Key(path));
  }
  
  public static void removeBlobMeta(CloudataConf conf, String path, BlobMetaCell blobMetaCell) throws IOException {
    CTable blobTable = CTable.openTable(conf, getBlobTableName(blobMetaCell.getTableName(), blobMetaCell.getColumnName()));
    
    Cell.Key cellKey = new Cell.Key(OFFSET_DF.format(blobMetaCell.getOffset()));

    //BlobMerger.LOG.info("Remove1: " + path + "," + cellKey);
    
    blobTable.remove(new Row.Key(path), Constants.BLOB_COLUMN_FILE, cellKey);
    blobTable.remove(new Row.Key(path), Constants.BLOB_COLUMN_DELETE, cellKey);
    
  }
  
  public static String getBlobRootPath(CloudataConf conf, String tableName, String columnName) {
    Calendar cal = Calendar.getInstance();
    int year = cal.get(Calendar.YEAR);
    int month = cal.get(Calendar.MONTH) + 1;
    
    String subdir = month >= 10 ? (year + "" + month) : (year + "0" + month);
    return conf.get("cloudata.root") + "/blobdata/files/" + tableName + "/" + columnName + "/" + subdir;
  }

  public static String getBlobTempPath(CloudataConf conf, String tableName, String columnName) {
    return conf.get("cloudata.root") + "/blobdata/temp/" + tableName + "/" + columnName;
  }
  
  public static String getBlobTableName(String tableName, String columnName) {
    return Constants.TABLE_NAME_BLOB_PREFIX + tableName + "_" + columnName;
  }

}
