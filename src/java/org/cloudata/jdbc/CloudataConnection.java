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

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;

import org.cloudata.core.common.conf.CloudataConf;


/**
 * @author jindolk
 *
 */
public class CloudataConnection implements Connection {
  private CloudataConf conf;

  private CloudataDatabaseMetaData metaData;
  
  String url;
  
  protected CloudataConnection(String url, CloudataConf conf) {
    this.conf = conf;
    this.metaData = new CloudataDatabaseMetaData(this);
    this.url = url;
  }
  
  public CloudataConf getConf() {
    return conf;
  }
  
  @Override
  public void clearWarnings() throws SQLException {
  }

  @Override
  public void close() throws SQLException {
  }

  @Override
  public void commit() throws SQLException {
  }

  @Override
  public Statement createStatement() throws SQLException {
    return new CloudataStatement(this);
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    return true;
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    return metaData;
  }

  @Override
  public String getCatalog() throws SQLException {
    return conf.get("pleiades.service.name");
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements)
      throws SQLException {
    throw new SQLException("Not support createArrayOf() operation");
  }

  @Override
  public Blob createBlob() throws SQLException {
    throw new SQLException("Not support createBlob() operation");
  }

  @Override
  public Clob createClob() throws SQLException {
    throw new SQLException("Not support createClob() operation");
  }

  @Override
  public NClob createNClob() throws SQLException {
    throw new SQLException("Not support createNClob() operation");
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    throw new SQLException("Not support createSQLXML() operation");
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException {
    throw new SQLException("Not support createStatement(resultSetType, resultSetConcurrency) operation");
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    throw new SQLException("Not support createStatement(resultSetType, resultSetConcurrency, resultSetHoldability) operation");
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes)
      throws SQLException {
    throw new SQLException("Not support createStruct() operation");
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    return null;
  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    return null;
  }

  @Override
  public int getHoldability() throws SQLException {
    return 0;
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    return 0;
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    return null;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    throw new SQLException("Not support getWarnings() operation");
  }

  @Override
  public boolean isClosed() throws SQLException {
    return false;
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return false;
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    return false;
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    throw new SQLException("Not support nativeSQL() operation");
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    throw new SQLException("Not support prepareCall() operation");
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType,
      int resultSetConcurrency) throws SQLException {
    throw new SQLException("Not support prepareCall() operation");
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType,
      int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    throw new SQLException("Not support prepareCall() operation");
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    throw new SQLException("Not support prepareStatement() operation");
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
      throws SQLException {
    throw new SQLException("Not support prepareStatement() operation");
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
      throws SQLException {
    throw new SQLException("Not support prepareStatement() operation");
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames)
      throws SQLException {
    throw new SQLException("Not support prepareStatement() operation");
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType,
      int resultSetConcurrency) throws SQLException {
    throw new SQLException("Not support prepareStatement() operation");
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType,
      int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    throw new SQLException("Not support prepareStatement() operation");
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
  }

  @Override
  public void rollback() throws SQLException {
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
  }

  @Override
  public void setClientInfo(Properties properties)
      throws SQLClientInfoException {
  }

  @Override
  public void setClientInfo(String name, String value)
      throws SQLClientInfoException {
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    throw new SQLException("Not support setSavepoint() operation");
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    throw new SQLException("Not support setSavepoint() operation");
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
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
