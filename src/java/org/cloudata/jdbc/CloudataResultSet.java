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
package org.cloudata.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudata.core.client.Cell;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.cql.statement.RowIterator;


/**
 * rowkey filed index: columns.length <BR>
 * getString() returns Cell.Key and getBytes() returns Cell.Value. getTimestamp() returns Cell.Value.timestamp <BR>
 * getObject() returns current cell. <BR>
 * next() move cursor to next Cell in a Row. If all Cell is null, move to next Row.  
 *
 */
public class CloudataResultSet implements ResultSet {
  private CloudataResultSetMetaData metaData;
  
  private RowIterator rowIterator;
  
  private Statement statement;
  
  private Row currentRow;
  
  private List<Cell>[] cells;
  
  private int currentCellIndex;
  
  private int maxCellIndex;
  
  private boolean closed = false;

  private String[] columns;
  
  private int rowNum;
  
  private Map<String, Integer> columnMap = new HashMap<String, Integer>();
  /**
   * @param rows
   */
  public CloudataResultSet(Statement statement, RowIterator rowIterator) throws SQLException {
    this.statement = statement;
    this.rowIterator = rowIterator;
    this.columns = rowIterator.getColumns();
    
    for(int i = 0; i < columns.length; i++) {
      columnMap.put(columns[i], i + 1);
    }
    
    this.cells = new List[columns.length];
    
    this.metaData = new CloudataResultSetMetaData(this);
  }


  @Override
  public boolean next() throws SQLException {
    currentCellIndex++;
    if(currentCellIndex >= maxCellIndex) {
      try {
        return initNextRow();
      } catch (IOException e) {
        throw new SQLException(e);
      }
    } else {
      return true;
    }
  }
  
  private boolean initNextRow() throws IOException {
    currentCellIndex = 0;
    
    maxCellIndex = 0;
    
    currentRow = null;
    if(rowIterator != null && rowIterator.hasNext()) {
      currentRow = rowIterator.nextRow();
    } else {
      return false;
    }
    
    if(currentRow != null) {
      for(int i = 0; i < cells.length; i++) {
        List<Cell> columnCells = currentRow.getCellList(columns[i]);
        if(columnCells != null && columnCells.size() > maxCellIndex) {
          maxCellIndex = columnCells.size();
        }
        cells[i] = columnCells;
      }
    } else {
      for(int i = 0; i < cells.length; i++) {
        cells[i] = null;
      }
      return false;
    }
    rowNum++;
    
    return true;
  }
  
  protected RowIterator getRowIterator() {
    return rowIterator;
  }
  
  private Cell getCell(int columnIndex) {
    int index = columnIndex - 1;
    if(cells[index] == null) {
      return null;
    }
    if(cells[index].size() > currentCellIndex) {
      return cells[index].get(currentCellIndex);
    } else {
      return null;
    }
  }
  
  @Override
  public void close() throws SQLException {
    if(closed) {
      return;
    }
    try {
      rowIterator.close();
      closed = true;
    } catch (IOException e) {
      throw new SQLException(e);
    }
  }
  
  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    Cell cell = getCell(columnIndex);
    if(cell == null) {
      return null;
    }
    if(cell.getBytes() != null) {
      return new BigDecimal(cell.getValueAsString()); 
    } else {
      return null;
    }
  }

  @Override
  public int findColumn(String columnLabel) throws SQLException {
    if(columnLabel.toLowerCase().equals("rowkey")) {
      return columns.length + 1;
    }
    if(!columnMap.containsKey(columnLabel)) {
      throw new SQLException("No column:" + columnLabel);
    }
    return columnMap.get(columnLabel);
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    Cell cell = getCell(columnIndex);
    if(cell == null) {
      return false;
    }
    if(cell.getBytes() != null) {
      return "true".equals(cell.getValueAsString()); 
    } else {
      return false;
    }
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    Cell cell = getCell(columnIndex);
    if(cell == null) {
      return null;
    }
    return cell.getBytes();
  }

  @Override
  public byte[] getBytes(String columnLabel) throws SQLException {
    return getBytes(findColumn(columnLabel));
  }
  
  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return metaData;
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    return getCell(columnIndex);
  }

  @Override
  public Object getObject(String columnLabel) throws SQLException {
    return getObject(findColumn(columnLabel));
  }

  @Override
  public int getRow() throws SQLException {
    return rowNum;
  }

  @Override
  public Statement getStatement() throws SQLException {
    return this.statement;
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    if(columnIndex == columns.length + 1) {
      return currentRow.getKey().toString();
    }
    
    Cell cell = getCell(columnIndex);
    if(cell != null) {
      return cell.getKey().toString();
    } else {
      return null;
    }
  }

  @Override
  public String getString(String columnLabel) throws SQLException {
    return getString(findColumn(columnLabel));
  }
  
  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    Cell cell = getCell(columnIndex);
    if(cell == null) {
      return null;
    }
    return new Timestamp(cell.getValue().getTimestamp());
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    return getTimestamp(findColumn(columnLabel));
  }

  @Override
  public boolean isClosed() throws SQLException {
    return closed;
  }

  @Override
  public boolean isFirst() throws SQLException {
    return rowNum == 1;
  }

  @Override
  public boolean isLast() throws SQLException {
    try {
      return rowIterator.hasNext();
    } catch (IOException e) {
      throw new SQLException(e); 
    }
  }

  @Override
  public boolean last() throws SQLException {
    if(closed) {
      return false;
    }
    try {
      while(rowIterator.hasNext()) {
        initNextRow();
      }
      
      this.currentCellIndex = this.maxCellIndex;
      
      return true;
    } catch (IOException e) {
      throw new SQLException(e);
    }
  }
    
  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal)
      throws SQLException {
    throw new SQLException("Not support getTimestamp(int columnIndex, Calendar cal) operation");
  }

  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar cal)
      throws SQLException {
    throw new SQLException("Not support getTimestamp(String columnLabel, Calendar cal) operation");
  }

  @Override
  public Object getObject(int columnIndex, Map<String, Class<?>> map)
      throws SQLException {
    throw new SQLException("Not support getObject(int columnIndex, Map<String, Class<?>> map) operation");
  }

  @Override
  public Object getObject(String columnLabel, Map<String, Class<?>> map)
      throws SQLException {
    throw new SQLException("Not support getObject(tring columnLabel, Map<String, Class<?>> map) operation");
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    throw new SQLException("Not support getBigDecimal() operation");
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale)
      throws SQLException {
    throw new SQLException("Not support getBigDecimal() operation");
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel, int scale)
      throws SQLException {
    throw new SQLException("Not support getBigDecimal() operation");
  }
  
  @Override
  public boolean getBoolean(String columnLabel) throws SQLException {
    throw new SQLException("Not support getBoolean() operation");
  }
  
  @Override
  public Date getDate(int columnIndex) throws SQLException {
    throw new SQLException("Not support getDate() operation");
  }

  @Override
  public Date getDate(String columnLabel) throws SQLException {
    throw new SQLException("Not support getDate() operation");
  }

  @Override
  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    throw new SQLException("Not support getDate() operation");
  }

  @Override
  public Date getDate(String columnLabel, Calendar cal) throws SQLException {
    throw new SQLException("Not support getDate() operation");
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    throw new SQLException("Not support getDouble() operation");
  }

  @Override
  public double getDouble(String columnLabel) throws SQLException {
    throw new SQLException("Not support getDouble() operation");
  }
  
  @Override
  public float getFloat(int columnIndex) throws SQLException {
    throw new SQLException("Not support getFloat() operation");
  }

  @Override
  public float getFloat(String columnLabel) throws SQLException {
    throw new SQLException("Not support getFloat() operation");
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    throw new SQLException("Not support getInt() operation");
  }

  @Override
  public int getInt(String columnLabel) throws SQLException {
    throw new SQLException("Not support getInt() operation");
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    throw new SQLException("Not support getLong() operation");
  }

  @Override
  public long getLong(String columnLabel) throws SQLException {
    throw new SQLException("Not support getLong() operation");
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    throw new SQLException("Not support getShort() operation");
  }

  @Override
  public short getShort(String columnLabel) throws SQLException {
    throw new SQLException("Not support getShort() operation");
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException {
    throw new SQLException("Not support getTime() operation");
  }

  @Override
  public Time getTime(String columnLabel) throws SQLException {
    throw new SQLException("Not support getTime() operation");
  }

  @Override
  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    throw new SQLException("Not support getTime() operation");
  }

  @Override
  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    throw new SQLException("Not support getTime() operation");
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    throw new SQLException("Not support getByte() operation");
  }

  @Override
  public byte getByte(String columnLabel) throws SQLException {
    throw new SQLException("Not support getByte() operation");
  }
  
  @Override
  public boolean absolute(int row) throws SQLException {
    throw new SQLException("Not support absolute() operation");
  }

  @Override
  public void afterLast() throws SQLException {
    throw new SQLException("Not support afterLast() operation");
  }

  @Override
  public void beforeFirst() throws SQLException {
    throw new SQLException("Not support beforeFirst() operation");
  }

  @Override
  public void cancelRowUpdates() throws SQLException {
    throw new SQLException("Not support cancelRowUpdates() operation");    
  }

  @Override
  public void clearWarnings() throws SQLException {
    throw new SQLException("Not support clearWarnings() operation");  
  }

  @Override
  public void deleteRow() throws SQLException {
    throw new SQLException("Not support deleteRow() operation");  
  }
  
  @Override
  public boolean first() throws SQLException {
    throw new SQLException("Not support first() operation");  
  }

  @Override
  public Array getArray(int columnIndex) throws SQLException {
    throw new SQLException("Not support getArray() operation");  
  }

  @Override
  public Array getArray(String columnLabel) throws SQLException {
    throw new SQLException("Not support getArray() operation"); 
  }

  @Override
  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    throw new SQLException("Not support getAsciiStream() operation"); 
  }

  @Override
  public InputStream getAsciiStream(String columnLabel) throws SQLException {
    throw new SQLException("Not support getAsciiStream() operation"); 
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    throw new SQLException("Not support getBinaryStream() operation"); 
  }

  @Override
  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    throw new SQLException("Not support getBinaryStream() operation"); 
  }

  @Override
  public Blob getBlob(int columnIndex) throws SQLException {
    throw new SQLException("Not support getBlob() operation"); 
  }

  @Override
  public Blob getBlob(String columnLabel) throws SQLException {
    throw new SQLException("Not support getBlob() operation"); 
  }

  @Override
  public Reader getCharacterStream(int columnIndex) throws SQLException {
    throw new SQLException("Not support getCharacterStream() operation"); 
  }

  @Override
  public Reader getCharacterStream(String columnLabel) throws SQLException {
    throw new SQLException("Not support getCharacterStream() operation"); 
  }

  @Override
  public Clob getClob(int columnIndex) throws SQLException {
    throw new SQLException("Not support getClob() operation"); 
  }

  @Override
  public Clob getClob(String columnLabel) throws SQLException {
    throw new SQLException("Not support getClob() operation"); 
  }

  @Override
  public int getConcurrency() throws SQLException {
    throw new SQLException("Not support getConcurrency() operation"); 
  }

  @Override
  public String getCursorName() throws SQLException {
    throw new SQLException("Not support getCursorName() operation"); 
  }

  @Override
  public int getFetchDirection() throws SQLException {
    throw new SQLException("Not support getFetchDirection() operation"); 
  }

  @Override
  public int getFetchSize() throws SQLException {
    throw new SQLException("Not support getFetchSize() operation"); 
  }

  @Override
  public int getHoldability() throws SQLException {
    throw new SQLException("Not support getHoldability() operation"); 
  }

  @Override
  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    throw new SQLException("Not support getNCharacterStream() operation"); 
  }

  @Override
  public Reader getNCharacterStream(String columnLabel) throws SQLException {
    throw new SQLException("Not support getNCharacterStream() operation"); 
  }

  @Override
  public NClob getNClob(int columnIndex) throws SQLException {
    throw new SQLException("Not support getNClob() operation"); 
  }

  @Override
  public NClob getNClob(String columnLabel) throws SQLException {
    throw new SQLException("Not support getNClob() operation"); 
  }

  @Override
  public String getNString(int columnIndex) throws SQLException {
    throw new SQLException("Not support getNString() operation"); 
  }

  @Override
  public String getNString(String columnLabel) throws SQLException {
    throw new SQLException("Not support getNString() operation"); 
  }

  @Override
  public Ref getRef(int columnIndex) throws SQLException {
    throw new SQLException("Not support getRef() operation"); 
  }

  @Override
  public Ref getRef(String columnLabel) throws SQLException {
    throw new SQLException("Not support getRef() operation"); 
  }

  @Override
  public RowId getRowId(int columnIndex) throws SQLException {
    throw new SQLException("Not support getRowId() operation"); 
  }

  @Override
  public RowId getRowId(String columnLabel) throws SQLException {
    throw new SQLException("Not support getRowId() operation"); 
  }

  @Override
  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    throw new SQLException("Not support getSQLXML() operation"); 
  }

  @Override
  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    throw new SQLException("Not support getSQLXML() operation"); 
  }

  @Override
  public int getType() throws SQLException {
    throw new SQLException("Not support getType() operation"); 
  }

  @Override
  public URL getURL(int columnIndex) throws SQLException {
    throw new SQLException("Not support getURL() operation");     
  }

  @Override
  public URL getURL(String columnLabel) throws SQLException {
    throw new SQLException("Not support getURL() operation");     
  }

  @Override
  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    throw new SQLException("Not support getUnicodeStream() operation");     
  }

  @Override
  public InputStream getUnicodeStream(String columnLabel) throws SQLException {
    throw new SQLException("Not support getUnicodeStream() operation");     
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    throw new SQLException("Not support getWarnings() operation");     
  }

  @Override
  public void insertRow() throws SQLException {
    throw new SQLException("Not support insertRow() operation");     
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    throw new SQLException("Not support isAfterLast() operation");
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    throw new SQLException("Not support isBeforeFirst() operation");
  }

  @Override
  public void moveToCurrentRow() throws SQLException {
    throw new SQLException("Not support moveToCurrentRow() operation");  
  }

  @Override
  public void moveToInsertRow() throws SQLException {
    throw new SQLException("Not support moveToInsertRow() operation");  
  }

  @Override
  public boolean previous() throws SQLException {
    throw new SQLException("Not support previous() operation");  
  }

  @Override
  public void refreshRow() throws SQLException {
    throw new SQLException("Not support refreshRow() operation");  
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    throw new SQLException("Not support relative() operation"); 
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    throw new SQLException("Not support rowDeleted() operation"); 
  }

  @Override
  public boolean rowInserted() throws SQLException {
    throw new SQLException("Not support rowInserted() operation"); 
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    throw new SQLException("Not support rowUpdated() operation"); 
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    throw new SQLException("Not support setFetchDirection() operation"); 
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#setFetchSize(int)
   */
  @Override
  public void setFetchSize(int rows) throws SQLException {
    throw new SQLException("Not support this operation."); 
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateArray(int, java.sql.Array)
   */
  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException {
    throw new SQLException("Not support this operation."); 
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateArray(java.lang.String, java.sql.Array)
   */
  @Override
  public void updateArray(String columnLabel, Array x) throws SQLException {
    throw new SQLException("Not support this operation."); 
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x)
      throws SQLException {
    throw new SQLException("Not support this operation."); 
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x)
      throws SQLException {
    throw new SQLException("Not support this operation.");   
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length)
      throws SQLException {
    throw new SQLException("Not support this operation.");   
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length)
      throws SQLException {
    throw new SQLException("Not support this operation.");     
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length)
      throws SQLException {
    throw new SQLException("Not support this operation.");     
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    throw new SQLException("Not support this operation.");     
  }

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x)
      throws SQLException {
    throw new SQLException("Not support this operation.");     
  }

  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x)
      throws SQLException {
    throw new SQLException("Not support this operation.");     
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x)
      throws SQLException {
    throw new SQLException("Not support this operation.");     
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x)
      throws SQLException {
    throw new SQLException("Not support this operation.");     
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length)
      throws SQLException {
    throw new SQLException("Not support this operation.");     
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length)
      throws SQLException {
    throw new SQLException("Not support this operation.");     
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length)
      throws SQLException {
    throw new SQLException("Not support this operation.");     
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    throw new SQLException("Not support this operation.");     
  }

  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    throw new SQLException("Not support this operation.");     
  }

  @Override
  public void updateBlob(String columnLabel, Blob x) throws SQLException {
    throw new SQLException("Not support this operation.");     
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream)
      throws SQLException {
    throw new SQLException("Not support this operation.");     
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream)
      throws SQLException {
    throw new SQLException("Not support this operation.");     
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream, long length)
      throws SQLException {
    throw new SQLException("Not support this operation.");     
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream,
      long length) throws SQLException {
    throw new SQLException("Not support this operation.");     
  }

  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    throw new SQLException("Not support this operation.");     
  }

  @Override
  public void updateBoolean(String columnLabel, boolean x) throws SQLException {
    throw new SQLException("Not support this operation.");     
  }

  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException {
    throw new SQLException("Not support this operation.");     
  }

  @Override
  public void updateByte(String columnLabel, byte x) throws SQLException {
    throw new SQLException("Not support this operation.");     
  }

  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    throw new SQLException("Not support this operation.");     
  }

  @Override
  public void updateBytes(String columnLabel, byte[] x) throws SQLException {
    throw new SQLException("Not support this operation.");     
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x)
      throws SQLException {
    throw new SQLException("Not support this operation.");     
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader)
      throws SQLException {
    throw new SQLException("Not support this operation.");     
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length)
      throws SQLException {
    throw new SQLException("Not support this operation.");     
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader,
      int length) throws SQLException {
    throw new SQLException("Not support this operation.");     
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length)
      throws SQLException {
    throw new SQLException("Not support this operation.");     
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader,
      long length) throws SQLException {
    throw new SQLException("Not support this operation.");     
  }

  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException {
    throw new SQLException("Not support this operation.");     
  }

  @Override
  public void updateClob(String columnLabel, Clob x) throws SQLException {
    throw new SQLException("Not support this operation.");     
  }

  @Override
  public void updateClob(int columnIndex, Reader reader) throws SQLException {
    throw new SQLException("Not support this operation.");     
  }

  @Override
  public void updateClob(String columnLabel, Reader reader) throws SQLException {
    throw new SQLException("Not support this operation.");     
  }

  @Override
  public void updateClob(int columnIndex, Reader reader, long length)
      throws SQLException {
    throw new SQLException("Not support this operation.");     
  }

  @Override
  public void updateClob(String columnLabel, Reader reader, long length)
      throws SQLException {
    throw new SQLException("Not support this operation."); 
  }

  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException {
    throw new SQLException("Not support this operation."); 
  }

  @Override
  public void updateDate(String columnLabel, Date x) throws SQLException {
    throw new SQLException("Not support this operation."); 
  }

  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException {
    throw new SQLException("Not support this operation."); 
  }

  @Override
  public void updateDouble(String columnLabel, double x) throws SQLException {
    throw new SQLException("Not support this operation."); 
  }

  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException {
    throw new SQLException("Not support this operation."); 
  }

  @Override
  public void updateFloat(String columnLabel, float x) throws SQLException {
    throw new SQLException("Not support this operation."); 
  }

  @Override
  public void updateInt(int columnIndex, int x) throws SQLException {
    throw new SQLException("Not support this operation."); 
  }

  @Override
  public void updateInt(String columnLabel, int x) throws SQLException {
    throw new SQLException("Not support this operation."); 
  }

  @Override
  public void updateLong(int columnIndex, long x) throws SQLException {
    throw new SQLException("Not support this operation."); 
  }

  @Override
  public void updateLong(String columnLabel, long x) throws SQLException {
    throw new SQLException("Not support this operation."); 
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x)
      throws SQLException {
    throw new SQLException("Not support this operation."); 
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader)
      throws SQLException {
    throw new SQLException("Not support this operation."); 
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length)
      throws SQLException {
    throw new SQLException("Not support this operation."); 
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader,
      long length) throws SQLException {
    throw new SQLException("Not support this operation."); 
  }

  @Override
  public void updateNClob(int columnIndex, NClob clob) throws SQLException {
    throw new SQLException("Not support this operation."); 
  }

  @Override
  public void updateNClob(String columnLabel, NClob clob) throws SQLException {
    throw new SQLException("Not support this operation."); 
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader) throws SQLException {
    throw new SQLException("Not support this operation."); 
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader)
      throws SQLException {
    throw new SQLException("Not support this operation."); 
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader, long length)
      throws SQLException {
    throw new SQLException("Not support this operation."); 
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader, long length)
      throws SQLException {
    throw new SQLException("Not support this operation."); 
  }

  @Override
  public void updateNString(int columnIndex, String string) throws SQLException {
    throw new SQLException("Not support this operation."); 
  }

  @Override
  public void updateNString(String columnLabel, String string)
      throws SQLException {
    throw new SQLException("Not support this operation."); 
  }

  @Override
  public void updateNull(int columnIndex) throws SQLException {
    throw new SQLException("Not support this operation."); 
  }

  @Override
  public void updateNull(String columnLabel) throws SQLException {
    throw new SQLException("Not support this operation."); 
  }

  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException {
    throw new SQLException("Not support this operation.");  
  }

  @Override
  public void updateObject(String columnLabel, Object x) throws SQLException {
    throw new SQLException("Not support this operation."); 
  }

  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength)
      throws SQLException {
    throw new SQLException("Not support this operation.");  
  }

  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength)
      throws SQLException {
    throw new SQLException("Not support this operation."); 
  }

  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException {
    throw new SQLException("Not support this operation."); 
  }

  @Override
  public void updateRef(String columnLabel, Ref x) throws SQLException {
    throw new SQLException("Not support this operation."); 
  }

  @Override
  public void updateRow() throws SQLException {
    throw new SQLException("Not support this operation."); 
  }

  @Override
  public void updateRowId(int columnIndex, RowId x) throws SQLException {
    throw new SQLException("Not support this operation."); 
  }

  @Override
  public void updateRowId(String columnLabel, RowId x) throws SQLException {
    throw new SQLException("Not support this operation."); 
  }

  @Override
  public void updateSQLXML(int columnIndex, SQLXML xmlObject)
      throws SQLException {
    throw new SQLException("Not support this operation."); 
  }

  @Override
  public void updateSQLXML(String columnLabel, SQLXML xmlObject)
      throws SQLException {
    throw new SQLException("Not support this operation."); 
  }

  @Override
  public void updateShort(int columnIndex, short x) throws SQLException {
    throw new SQLException("Not support updateShort() operation"); 
  }

  @Override
  public void updateShort(String columnLabel, short x) throws SQLException {
    throw new SQLException("Not support updateShort() operation"); 
  }

  @Override
  public void updateString(int columnIndex, String x) throws SQLException {
    throw new SQLException("Not support updateString() operation"); 
  }

  @Override
  public void updateString(String columnLabel, String x) throws SQLException {
    throw new SQLException("Not support updateString() operation"); 
  }

  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException {
    throw new SQLException("Not support updateTimestamp() operation"); 
  }

  @Override
  public void updateTime(String columnLabel, Time x) throws SQLException {
    throw new SQLException("Not support updateTimestamp() operation"); 
  }

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    throw new SQLException("Not support updateTimestamp() operation"); 
  }

  @Override
  public void updateTimestamp(String columnLabel, Timestamp x)
      throws SQLException {
    throw new SQLException("Not support updateTimestamp() operation"); 
  }

  @Override
  public boolean wasNull() throws SQLException {
    throw new SQLException("Not support wasNull() operation"); 
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new SQLException("Not support isWrapperFor() operation"); 
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLException("Not support unwrap() operation"); 
  }
}
