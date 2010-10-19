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
package org.cloudata.core.rest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.tablet.TableSchema;


/**
 * @author doe-nam kim
 *
 */
public class CRestTable {
  private List<String> webServers = new ArrayList<String>(); 
  private int port;
  private Random rand = new Random();
  
  public CRestTable(CloudataConf conf) {
    this.port = conf.getInt("rest.server.port", 7003);
    String hosts = conf.get("rest.server.hosts", "localhost");
    
    for(String eachHost: hosts.split(",")) {
      webServers.add(eachHost.trim());
    }
  }
  
  public String listTables() throws IOException {
    HttpClient client = new HttpClient();
    GetMethod method = new GetMethod(selectRestServer() + "/table");
    int result = client.executeMethod(method);
    if(result != 200) { 
      throw new IOException(method.getResponseBodyAsString());
    }
    return method.getResponseBodyAsString();
  }
  
  public String descTable(String tableName) throws IOException {
    HttpClient client = new HttpClient();
    GetMethod method = new GetMethod(selectRestServer() + "/table/" + tableName);
    int result = client.executeMethod(method);
    if(result != 200) { 
      throw new IOException(method.getResponseBodyAsString());
    }
    return method.getResponseBodyAsString();
  }
  
  public String createTable(String tableName, String description, String[] columnNames) throws IOException {
    HttpClient client = new HttpClient();
    PostMethod method = new PostMethod(selectRestServer() + "/table/" + tableName);
    
    method.addParameter("data", CloudataRestService.getTableSchemaXml(new TableSchema(tableName, description, columnNames)));
    
    int result = client.executeMethod(method);
    if(result != 201) { 
      throw new IOException(method.getResponseBodyAsString());
    }
    return method.getResponseBodyAsString();
  }
  
  public String dropTable(String tableName) throws IOException {
    HttpClient client = new HttpClient();
    DeleteMethod method = new DeleteMethod(selectRestServer() + "/table/" + tableName);
    
    int result = client.executeMethod(method);
    if(result != 200) { 
      throw new IOException(method.getResponseBodyAsString());
    }
    return method.getResponseBodyAsString();
  }
  
  public String putRow(String tableName, Row row) throws IOException {
    HttpClient client = new HttpClient();
    PostMethod method = new PostMethod(selectRestServer() + "/" + tableName);
    
    method.addParameter("data", CloudataRestService.getRowXml(new Row[]{row}));
    
    int result = client.executeMethod(method);
    if(result != 201) { 
      throw new IOException(method.getResponseBodyAsString());
    }
    return method.getResponseBodyAsString();
  }
  
  public String get(String tableName, String rowKey) throws IOException {
    HttpClient client = new HttpClient();
    GetMethod method = new GetMethod(selectRestServer() + "/" + tableName + "/" + rowKey);
    int result = client.executeMethod(method);
    if(result != 200) { 
      throw new IOException(method.getResponseBodyAsString());
    }
    return method.getResponseBodyAsString();
  }
  
  public String delete(String tableName, String rowKey) throws IOException {
    HttpClient client = new HttpClient();
    DeleteMethod method = new DeleteMethod(selectRestServer() + "/" + tableName + "/" + rowKey);
    int result = client.executeMethod(method);
    if(result != 200) { 
      throw new IOException(method.getResponseBodyAsString());
    }
    return method.getResponseBodyAsString();
  }
  
  private String selectRestServer() throws IOException {
    if(webServers.size() == 0) {
      throw new IOException("No REST Server. check rest.server.hosts property in cloudata-default.xml or cloudata-site.xml");
    }
    
    return "http://" + webServers.get(rand.nextInt(webServers.size())) + ":" + port;
  }
  
  public static void main(String[] args) throws Exception {
  }
}
