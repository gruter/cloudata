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

import org.cloudata.core.client.Row;
import org.cloudata.core.client.Row.Key;
import org.cloudata.core.common.AsyncCallProtocol;
import org.cloudata.core.common.ipc.CVersionedProtocol;
import org.cloudata.core.tablet.TableSchema;
import org.cloudata.core.tablet.TabletInfo;
import org.cloudata.core.tablet.TableSchema.ColumnInfo;
import org.cloudata.core.tabletserver.TabletServerInfo;


/**
 * Table을 관리(drop, create)하기 위한 프로토콜
 * @author babokim
 *
 */
public interface TableManagerProtocol extends CVersionedProtocol, AsyncCallProtocol {
	public static final long versionID = 1L;
  
	/**
   * 테이블을 생성한다. 
   * endRowKeys가 null이 아닌 경우 테이블 생성 시 endRowKeys를 이용하여 테이블을 파티셔닝한다.
   * @param table
   * @param endRowKeys
   * @throws IOException
	 */
  public void createTable(TableSchema table, Row.Key[] endRowKeys) throws IOException;
	
	/** 
	 * 테이블을 drop한다. 테이블 drop 명령의 경우 RPC timeout내에 처리 되지 않기 때문에
   * async 작업으로 처리된다. 결과 값이 return 되어도 drop 처리가 종료된 것이 아니다.
	 * @param tableName
   * @retrun drop 요청된 Async Task id
	 * @throws IOException 
	 */
	public String dropTable(String tableName) throws IOException;
	
	/**
	 * 테이블 목록을 조회한다.
	 * @return
	 */
	public TableSchema[] listTables() throws IOException;

	/**
	 * 특정 테이블의 Tablet 목록을 조회한다.
	 * @param tableName
	 * @return
	 */
	public TabletInfo[] getTablets(String tableName) throws IOException;

  /**
   * 테이블에 새로운 Tablet을 추가한다. 
   * @param tableName
   * @param endRowKey
   * @return
   * @throws IOException
   */
  public TabletInfo addTablet(String tableName, Row.Key startRowKey, Row.Key endRowKey) throws IOException;
  
  /**
   * 테이블에 Column을 추가한다.
   * @param tableName
   * @param addedColumnName
   * @throws IOException
   */
public void addColumn(String tableName, String addedColumnName) throws IOException;
  
  public void test();
  
  public TabletServerInfo[] getTabletServerInfos();
  
  public void addUser(String userId) throws IOException;
  
  public void removeUser(String userId) throws IOException;
  
  public void addTablePermission(String tableName, String userId, String readWrite) throws IOException;
  
  public void removeTablePermission(String tableName, String userId) throws IOException;
  
  public void startBalancer() throws IOException;

//  public void splitTablet(String tableName, TabletInfo tabletInfo, Key[] rowKeys) throws IOException;
}

