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
package org.cloudata.thrift;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.tablet.TableSchema;
import org.cloudata.thrift.generated.ThriftIOException;
import org.cloudata.thrift.generated.ThriftRow;
import org.cloudata.thrift.generated.ThriftTableSchema;
import org.cloudata.thrift.generated.*;

/**
 * @author jindolk
 *
 */
public class CloudataThriftServer {
  public static class CloudataHandler implements ThriftCloudataService.Iface {
    CloudataConf conf;
    
    public CloudataHandler() {
      conf = new CloudataConf();
    }
    
    @Override
    public void addColumn(String tableName, String addedColumnName)
        throws ThriftIOException, TException {
      
    }

    @Override
    public void createTable(ThriftTableSchema tableSchema)
        throws ThriftIOException, TException {
      try {
        CTable.createTable(conf, ThriftUtil.getNTableSchema(tableSchema));
      } catch (IOException e) {
        throw new ThriftIOException(e.getMessage());
      }
    }

    @Override
    public ThriftTableSchema descTable(String tableName)
        throws ThriftIOException, TException {
      try {
        CTable ctable = CTable.openTable(conf, tableName);
        if(ctable == null) {
          throw new IOException("No table:" + tableName);
        }
        return ThriftUtil.getTTableSchema(ctable.descTable());
      } catch (IOException e) {
        throw new ThriftIOException(e.getMessage());
      }
    }

    @Override
    public void dropTable(String tableName) throws ThriftIOException,
        TException {
      try {
        CTable.dropTable(conf, tableName);
      } catch (IOException e) {
        throw new ThriftIOException(e.getMessage());
      }
    }

    @Override
    public boolean existsTable(String tableName) throws ThriftIOException,
        TException {
      try {
        return CTable.existsTable(conf, tableName);
      } catch (IOException e) {
        throw new ThriftIOException(e.getMessage());
      }
    }

    @Override
    public ThriftRow get(String tableName, String rowKey) throws ThriftIOException, TException {
      try {
        CTable ctable = CTable.openTable(conf, tableName);
        if(ctable == null) {
          throw new IOException("No table:" + tableName);
        }
        return ThriftUtil.getTRow(ctable.get(new Row.Key(rowKey)));
      } catch (IOException e) {
        throw new ThriftIOException(e.getMessage());
      }
    }

    @Override
    public ThriftRow getColumnRow(String tableName, String rowKey, List<String> columnNames)
        throws ThriftIOException, TException {
      try {
        CTable ctable = CTable.openTable(conf, tableName);
        if(ctable == null) {
          throw new IOException("No table:" + tableName);
        }
        return ThriftUtil.getTRow(ctable.get(new Row.Key(rowKey), columnNames.toArray(new String[]{})));
      } catch (IOException e) {
        throw new ThriftIOException(e.getMessage());
      }
    }

    @Override
    public byte[] getValue(String tableName, String rowKey, String columnName, String cellKey)
        throws ThriftIOException, TException {
      try {
        CTable ctable = CTable.openTable(conf, tableName);
        if(ctable == null) {
          throw new IOException("No table:" + tableName);
        }
        return ctable.get(new Row.Key(rowKey), columnName, new Cell.Key(cellKey));
      } catch (IOException e) {
        throw new ThriftIOException(e.getMessage());
      }
    }

    @Override
    public boolean hasValue(String tableName, String columnName, String rowKey)
        throws ThriftIOException, TException {
      try {
        CTable ctable = CTable.openTable(conf, tableName);
        if(ctable == null) {
          throw new IOException("No table:" + tableName);
        }
        return ctable.hasValue(columnName,new Row.Key(rowKey));
      } catch (IOException e) {
        throw new ThriftIOException(e.getMessage());
      }
    }

    @Override
    public List<ThriftTableSchema> listTables() throws ThriftIOException,
        TException {
      try {
        TableSchema[] tables = CTable.listTables(conf);
        
        List<ThriftTableSchema> result = new ArrayList<ThriftTableSchema>();
        for(TableSchema eachTable: tables) {
          result.add(ThriftUtil.getTTableSchema(eachTable));
        }
        
        return result;
      } catch (IOException e) {
        throw new ThriftIOException(e.getMessage());
      }
    }

    @Override
    public void put(String tableName, ThriftRow row, boolean useSystemTimestamp) throws ThriftIOException, TException {
      try {
        CTable ctable = CTable.openTable(conf, tableName);
        if(ctable == null) {
          throw new IOException("No table:" + tableName);
        }
        ctable.put(ThriftUtil.getNRow(row), useSystemTimestamp);
      } catch (IOException e) {
        throw new ThriftIOException(e.getMessage());
      }      
    }

    @Override
    public void remove(String tableName, String rowKey, String columnName, String cellKey)
        throws ThriftIOException, TException {
      try {
        CTable ctable = CTable.openTable(conf, tableName);
        ctable.remove(new Row.Key(rowKey), columnName, new Cell.Key(cellKey));
      } catch (IOException e) {
        throw new ThriftIOException(e.getMessage());
      }  
    }

    @Override
    public void removeRow(String tableName, String rowKey) throws ThriftIOException, TException {
      try {
        CTable ctable = CTable.openTable(conf, tableName);
        ctable.removeRow(new Row.Key(rowKey));
      } catch (IOException e) {
        throw new ThriftIOException(e.getMessage());
      }
    }

    @Override
    public void removeRowWithTime(String tableName, String rowKey, String timestamp)
        throws ThriftIOException, TException {
      try {
        CTable ctable = CTable.openTable(conf, tableName);
        ctable.removeRow(new Row.Key(rowKey), Long.parseLong(timestamp));
      } catch (IOException e) {
        throw new ThriftIOException(e.getMessage());
      }      
    }

    @Override
    public void truncateColumn(String tableName, String columnName)
        throws ThriftIOException, TException {
      try {
        CTable ctable = CTable.openTable(conf, tableName);
        if(ctable == null) {
          throw new IOException("No table:" + tableName);
        }
        ctable.truncateColumn(columnName);
      } catch (IOException e) {
        throw new ThriftIOException(e.getMessage());
      }    
    }

    @Override
    public void truncateTable(String tableName, boolean clearPartitionInfo)
        throws ThriftIOException, TException {
      try {
        CTable ctable = CTable.openTable(conf, tableName);
        if(ctable == null) {
          throw new IOException("No table:" + tableName);
        }
        ctable.truncateTable(clearPartitionInfo);
      } catch (IOException e) {
        throw new ThriftIOException(e.getMessage());
      }  
    }
  }
  
  public static void main(String [] args) {
    try {
      CloudataConf conf = new CloudataConf();
      
      CloudataHandler handler = new CloudataHandler();
      ThriftCloudataService.Processor processor = new ThriftCloudataService.Processor(handler);
      TServerTransport serverTransport = new TServerSocket(conf.getInt("thrift.server.port", 7002));
      TServer server = new TThreadPoolServer(processor, serverTransport);

      System.out.println("Cloudata thrift server started on " + conf.getInt("thrift.server.port", 7002) + " port");
      server.serve();

    } catch (Exception x) {
      x.printStackTrace();
    }
    System.out.println("done.");
  }
}
