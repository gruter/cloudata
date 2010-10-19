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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

/**
 * @author jindolk
 *
 */
public class CloudataResultSetMetaData implements ResultSetMetaData {
  private CloudataResultSet resultSet;
  
  private String[] columns;
  
  CloudataResultSetMetaData(CloudataResultSet resultSet) {
    this.resultSet = resultSet;
    this.columns = resultSet.getRowIterator().getColumns();
  }
    
  @Override
  public String getCatalogName(int column) throws SQLException {
    return resultSet.getStatement().getConnection().getCatalog();
  }

  @Override
  public String getColumnClassName(int column) throws SQLException {
    return String.class.getName();
  }

  @Override
  public int getColumnCount() throws SQLException {
    return columns.length;
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    return columns[column - 1].getBytes().length;
  }

  @Override
  public String getColumnLabel(int column) throws SQLException {
    return columns[column - 1];
  }

  @Override
  public String getColumnName(int column) throws SQLException {
    return columns[column - 1];
  }

  @Override
  public int getColumnType(int column) throws SQLException {
    return Types.VARCHAR;
  }

  @Override
  public String getColumnTypeName(int column) throws SQLException {
    return "String";
  }

  @Override
  public int getPrecision(int column) throws SQLException {
    return 0;
  }

  @Override
  public int getScale(int column) throws SQLException {
    return 0;
  }

  @Override
  public String getSchemaName(int column) throws SQLException {
    return "";
  }

  @Override
  public String getTableName(int column) throws SQLException {
    return resultSet.getRowIterator().getCTable().getTableName();
  }

  @Override
  public boolean isAutoIncrement(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isCaseSensitive(int column) throws SQLException {
    return true;
  }

  @Override
  public boolean isCurrency(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isDefinitelyWritable(int column) throws SQLException {
    return false;
  }

  @Override
  public int isNullable(int column) throws SQLException {
    return columnNullableUnknown;
  }

  @Override
  public boolean isReadOnly(int column) throws SQLException {
    return true;
  }

  @Override
  public boolean isSearchable(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isSigned(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isWritable(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return null;
  }
}