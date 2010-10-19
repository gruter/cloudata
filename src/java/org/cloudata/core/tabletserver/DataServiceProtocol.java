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
package org.cloudata.core.tabletserver;

import java.io.IOException;

import org.cloudata.core.client.Cell;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.RowFilter;
import org.cloudata.core.client.Row.Key;
import org.cloudata.core.common.ipc.CVersionedProtocol;
import org.cloudata.core.tablet.ColumnValue;
import org.cloudata.core.tablet.RowColumnValues;
import org.cloudata.core.tablet.TabletInfo;
import org.cloudata.core.tablet.TabletReport;


public interface DataServiceProtocol extends CVersionedProtocol {
  public static final long versionID = 1L;
  /**
   * 
   * @param tabletName
   * @return
   */
  public TabletInfo getTabletInfo(String tabletName);
  
  /**
   * TabletServer가 특정 tablet에 대해 DataServer를 하고 있는지 여부를 반환한다.
   * @param tabletName
   * @return
   */
  public boolean isServicedTablet(String tabletName);
  
  public long apply(String tabletName, Row.Key rowKey, TxId txId, CommitLog[] commitLogList, boolean saveLog) throws IOException;

  public boolean startFlush(String tabletName, TxId txId) throws IOException;
  
  public void endFlush(String tabletName, TxId txId) throws IOException;
  
  /**
   * 
   * @param tabletName
   * @param rowKey
   * @param columnName
   * @param columnKey
   * @return
   */
  public ColumnValue get(String tabletName, Row.Key rowKey, 
                                   String columnName, Cell.Key columnKey) throws IOException;
  
  /**
   * 
   * @param tabletName
   * @param rowKey
   * @return
   * @throws IOException 
   */
  public ColumnValue[][] get(String tabletName, Row.Key rowKey) throws IOException;
  
  /**
   * 
   * @param tabletName
   * @param rowKey
   * @param columnNames
   * @return
   * @throws IOException 
   */
  public ColumnValue[][] get(String tabletName, Row.Key rowKey, String[] columnNames) throws IOException;
  
  /**
   * 해당 rowkey보다 작은 레코드 중 가장 큰 레코드를 조회한다.
   * 주로 meta tablet에서 특정 rowkey를 서비스하고 있는 사용자 tablet에 대한 정보를 조회할 때 사용한다.
   * @param tabletName
   * @param rowKey
   * @param columnNames
   * @return
   * @throws IOException 
   */
  public ColumnValue getCloestMetaData(String tabletName, Row.Key rowKey) throws IOException;

  /**
   * 
   * @param tabletName
   * @return
   * @throws IOException
   */
  public String[] getAllActions(String tabletName) throws IOException;
  
  public String getTabletServerConf(String key) throws IOException;
  
  public String test(long sleepTime, String echo) throws IOException;

  public void saveTabletSplitedInfo(String tabletName, TabletInfo targetTablet, TabletInfo[] splitedTablets) throws IOException;
  
  public void printMeta(String tabletName) throws IOException;

  public void stopAction(String tableName, String tabletActionClassName) throws IOException;
  
  public TabletReport getTabletDetailInfo(String tabletName) throws IOException;

  public boolean canUpload() throws IOException;

  public RowColumnValues[] get(String tabletName, RowFilter scanFilter) throws IOException;
  
  public String startBatchUploader(String tabletName, Row.Key rowKey, boolean touch) throws IOException;

  public AsyncTaskStatus getAsyncTaskStatus(String taskId) throws IOException;
  
  public String endBatchUploader(String actionId, 
      String tabletName, 
      String[] columnNames, 
      String[] mapFileIds, 
      String[] mapFilePaths) throws IOException;

  public void cancelBatchUploader(String actionId, String tabletName) throws IOException;
  
  /**
   * Client API에서 강제적으로 TabletServer에 있는 lease 항목을 touch 한다.
   * 주로 Scanner에서 scanner가 close 되기 전까지 open 되도록 하는데 사용
   * @param leaseId
   * @throws IOException
   */
  public void touch(String leaseId) throws IOException;
  
  public ColumnValue[][] testRPC();
  
  /**
   * ROOT, META 테이블 검증용으로 사용된다.
   * 해당 메소드가 호출되는 경우에는 Split 등의 작업이 lock이 걸리기 때문에
   * 메모리의 값에 대한 변경이 거의 없다.
   * @param tabletName
   * @param columnName
   * @return
   */
  public ColumnValue[] getAllMemoryValues(String tabletName, String columnName) throws IOException;
  
  /**
   * Tablet에 rowKey 값이 존재하는 지 반환한다.
   * @param tabletName
   * @param columnName
   * @param rowKey
   * @param cellKey
   * @return
   * @throws IOException
   */
  public boolean hasValue(String tabletName, String columnName, Row.Key rowKey, Cell.Key cellKey) throws IOException;

  /**
   * TabletInputFormat에서 사용하는 메소드로 Tablet의 rowkey 목록에서 splitPerTablet 갯수만큼 범위를
   * 나누어 각 범위의 마지막 rowkey 값을 반환한다. map 갯수 만큼 input 영역을 나누기 위해서 사용한다.  
   * @param tabletName
   * @param splitPerTablet
   * @return
   */
  public Row.Key[] getSplitedRowKeyRanges(String tabletName, int splitPerTablet) throws IOException;

  /**
   * Like, Great than, Less than 등과 같은 multi row operation을 처리한다.
   * @param tabletName
   * @param startRowKey
   * @param rowFilter
   * @return
   */
  public RowColumnValues[][] gets(String tabletName, Key startRowKey,
      RowFilter rowFilter) throws IOException;
  
  public ColumnValue[] getColumnMemoryCacheDatas(String tabletName, String columnName) throws IOException;
  
}
