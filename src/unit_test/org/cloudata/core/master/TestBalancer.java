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
package org.cloudata.core.master;

import java.io.IOException;
import java.text.DecimalFormat;

import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.tablet.TableSchema;


/**
 * @author jindolk
 *
 */
public class TestBalancer {
  public static void main(String[] args) throws IOException {
    CloudataConf conf = new CloudataConf();
    
    TableSchema tableSchema = new TableSchema("T_TEST", "test", new String[]{"Col1"});
    int partitionCount = 1000;
    DecimalFormat df = new DecimalFormat("0000000000");
    
    Row.Key[] rowKeys = new Row.Key[partitionCount]; 
    for(int i = 0; i < partitionCount - 1; i++) {
      rowKeys[i] = new Row.Key(df.format(i));
    }
    rowKeys[partitionCount - 1] = Row.Key.MAX_KEY;
    
    CTable.createTable(conf, tableSchema, rowKeys);
  }
}
