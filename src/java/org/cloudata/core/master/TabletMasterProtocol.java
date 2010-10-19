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

import org.cloudata.core.commitlog.ServerMonitorInfo;
import org.cloudata.core.common.ipc.CVersionedProtocol;
import org.cloudata.core.tablet.TabletInfo;


/**
 * Tablet을 관리하기 위한 프로토콜
 * @author babokim
 *
 */
public interface TabletMasterProtocol extends CVersionedProtocol {
	public static final long versionID = 1L;
  
  /**
   * 
   * @param tabletInfo
   * @param ignoreFlag
   * @return
   * @throws IOException
   */
  public boolean assignTablet(TabletInfo tabletInfo) throws IOException;
  
	/**
	 * 
	 * @param tabletInfo
	 * @param created
	 * @throws IOException 
	 */
	public void endTabletAssignment(TabletInfo tabletInfo, boolean created) throws IOException;
	
	/**
	 * 
	 * @return
	 */
	public boolean checkServer();

	/**
	 * 
	 * @param tabletInfo
	 */
	public void errorTabletAssignment(String hostName, TabletInfo tabletInfo);
  
  /**
   * 
   * @param tabletInfo
   */
  //public void endReadyForStop(TabletInfo tabletInfo) throws IOException ;

  /**
   * 
   * @param targetTablet
   * @param splitedTablets
   */
  public void reportTabletSplited(TabletInfo targetTablet, TabletInfo[] splitedTablets);

  public void errorTableDrop(String taskId, String hostName, String tableName, String message) throws IOException;

  public void endTableDrop(String taskId, String hostName, String tableName) throws IOException;
  
  public void heartbeatTS(String hostName, int tabletNum) throws IOException;
  
  public void heartbeatCS(String hostName, ServerMonitorInfo serverMonitorInfo) throws IOException;
  
  public void doRebalacing(String tabletServerHostName, TabletInfo tabletInfo, boolean end) throws IOException;
}
