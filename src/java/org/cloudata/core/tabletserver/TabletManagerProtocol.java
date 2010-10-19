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

import org.cloudata.core.common.ipc.CVersionedProtocol;
import org.cloudata.core.tablet.TabletInfo;
import org.cloudata.core.tablet.TabletReport;


public interface TabletManagerProtocol extends CVersionedProtocol {
	public static final long versionID = 1L;
	/**
	 * 
	 * @param tabletInfo
	 */
	public void assignTablet(TabletInfo tabletInfo) throws IOException;
	
	/**
	 * 
	 * @return
	 */
	public TabletInfo[] reportTablets();
	
	/**
	 * 
	 * @param TabletName
	 * @throws IOException 
	 */
	public void printTabletInfo(String tabletName) throws IOException;

	/**
	 * 
	 *
	 */
	public boolean checkServer();
	
  public void splitForTest(TabletInfo tabletInfo) throws IOException;
  
  public void stopAllTablets() throws IOException;
  
  public void truncateColumn(String tabletName, String columnName) throws IOException;

  public void addColumn(String tableName, String tabletName, String addedColumnName) throws IOException;
  
  public void doActionForTest(String tabletName, String action) throws IOException;
  
  public boolean dropTable(String taskId, String tableName) throws IOException;
  
  public void setProperty(String key, String value) throws IOException;

  public String getProperty(String key, String defaultValue) throws IOException;

  public void shutdown() throws IOException;

  public TabletInfo getTabletInfo(String tabletName) throws IOException;
  
  public TabletServerStatus getServerStatus() throws IOException;
  
  public TabletReport getTabletDetailInfo(String tabletName) throws IOException;

  public void doAllTabletMinorCompaction() throws IOException;

  public void electRebalacingTablets(int targetTabletNumPerTabletServer) throws IOException;
}
