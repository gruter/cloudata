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
package org.cloudata.core.parallel.hadoop;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.cloudata.core.client.CellFilter;
import org.cloudata.core.client.RowFilter;

/**
 * Basic AbstractTabletInputFormat. isRowScan() method return true.<BR>
 * Must be set the following JobConf's properties.<BR>
 * <ul>
 *  <li>AbstractTabletInputFormat.INPUT_TABLE</li>
 *  <li>AbstractTabletInputFormat.INPUT_COLUMN_LIST: ",&#8221; deliminator</li>
 * </ul>
 */
public class DefaultTabletInputFormat extends AbstractTabletInputFormat {
  public DefaultTabletInputFormat() throws IOException {
    super();
  }

  @Override
  public InputTableInfo[] getInputTableInfos(JobConf jobConf) {
    String tableName = jobConf.get(AbstractTabletInputFormat.INPUT_TABLE);
    String[] columns = jobConf.get(AbstractTabletInputFormat.INPUT_COLUMN_LIST).split(",");
    RowFilter rowFilter = new RowFilter();
    for(String eachColumn: columns) {
      rowFilter.addCellFilter(new CellFilter(eachColumn));
    }
    
    InputTableInfo inputTableInfo = new InputTableInfo();
    inputTableInfo.setTable(tableName, rowFilter);
    
    return new InputTableInfo[]{inputTableInfo};

  }
}