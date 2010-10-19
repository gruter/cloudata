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
import java.io.PrintWriter;
import java.io.Writer;
import java.lang.reflect.Method;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import org.apache.zookeeper.ZooKeeper;
import org.cloudata.core.client.CellFilter;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.RowFilter;
import org.cloudata.core.commitlog.CommitLogServerIF;
import org.cloudata.core.commitlog.ServerMonitorInfo;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.ipc.CRPC;
import org.cloudata.core.common.lock.LockUtil;
import org.cloudata.core.common.util.NetworkUtil;
import org.cloudata.core.fs.DiskInfo;
import org.cloudata.core.tablet.TableSchema;
import org.cloudata.core.tablet.TabletInfo;
import org.cloudata.core.tablet.TabletReport;
import org.cloudata.core.tablet.TableSchema.ColumnInfo;
import org.cloudata.core.tabletserver.DataServiceProtocol;
import org.cloudata.core.tabletserver.TabletManagerProtocol;
import org.cloudata.core.tabletserver.TabletServerInfo;
import org.cloudata.core.tabletserver.TabletServerStatus;


public class JspHelper {
  CloudataConf conf;
  ZooKeeper zk;
  DecimalFormat df;
  
  public JspHelper() throws IOException {
    conf = new CloudataConf();
    zk = LockUtil.getZooKeeper(conf, "web_admin", null);
    df = new DecimalFormat("###,###,###,###");
  }
  
  public String getMasterHostName() throws IOException {
    return CloudataMaster.getMasterServerHostName(conf, zk);
  }
  
  public void getConfig(Writer out) throws IOException {
    CloudataConf conf = CloudataMaster.cloudataMaster.getConf();
    
    Properties pros = conf.getProps();
    
    List<String> keys = new ArrayList<String>();
    for(Object key: pros.keySet()) {
      keys.add(key.toString());
    }
    
    Collections.sort(keys);
    
    out.write("<table border=\"1\" width=\"100%\">");
    for(String eachKey: keys) {
      out.write("<tr><td>" + eachKey + "</td><td>" + pros.getProperty(eachKey) + "</td></tr>");
    }
    out.write("</table>");
  }
  
  public void getClusterInfo(Writer out) throws IOException {
    out.write("<table border=\"0\">");
    out.write("<tr><td width=\"100\"><b>Started:</b></td><td>" + CloudataMaster.cloudataMaster.masterStartTime + "</td></tr>");
    out.write("<tr><td width=\"100\"><b>Master Lock:</b></td><td>" + CloudataMaster.cloudataMaster.masterInitTime + "</td></tr>");
    out.write("<tr><td width=\"100\"><b>Version:</b></td><td>" + 
        Constants.MAJOR_VERSION + "." + Constants.MINOR_VERSION + "." + Constants.MINOR_VERSION_2 + "</td></tr>");
    
    long free = Runtime.getRuntime().freeMemory();
    long max = Runtime.getRuntime().maxMemory();
    long total = Runtime.getRuntime().totalMemory();
    
    String memory = df.format(free/1024) + "/" + df.format(total/1024) + " KB, max=" + df.format(max/1024) + " KB";
    
    String lockStatus = "";
    try {
      lockStatus = CloudataMaster.cloudataMaster.getLockStatus();
    } catch (IOException e) {
      lockStatus = "Error: " + e.getMessage();
    }
    out.write("<tr><td width=\"100\"><b>Heap:</b></td><td>" + memory + "</td></tr>");
    out.write("<tr><td width=\"100\"><b>Load:</b></td><td>" + lockStatus + "</td></tr>");
    out.write("<tr><td width=\"100\"><b>Configuration:</b></td><td><a href=\"config.jsp\">master config</a></td></tr>");
    out.write("</table>");
  }

  public void getCommitLogServers(Writer out) throws IOException {
    Map<String, ServerMonitorInfo> addrs = new TreeMap<String, ServerMonitorInfo>();
    addrs.putAll(CloudataMaster.cloudataMaster.getLiveCommitLogServers());
    
    out.write("<h3>Live CommitLogServers: " + addrs.size() + "</h3>");
    if(addrs.size() == 0) {
      return;
    }
    out.write("<table border=\"1\" width=\"100%\">");
    out.write("<tr><th>Node</th><th>Last<br/>Contact</th><th># Logs</th><th>Capacity<br/>(MB)</th><th>Used</th><th>Logs<br/>(MB)</th><th>Available</th><th>Use%</th></tr>");

    long systemTime = System.currentTimeMillis();
    
    for(Map.Entry<String, ServerMonitorInfo> entry: addrs.entrySet()) {
      String addr = entry.getKey();
      ServerMonitorInfo serverInfo = entry.getValue();
      out.write("<tr>");
      out.write("<td><a href=\"commitlogserver.jsp?server=" + addr + "\">" + addr + "</a></td>");
      out.write("<td>" + (systemTime - serverInfo.getLastHeartbeatTime())/1000 + "</td>");
      if(serverInfo != null) {
        DiskInfo diskInfo = serverInfo.getDiskInfo();
        out.write("<td>" + serverInfo.getLogFiles().size() + "</td>");
        if(diskInfo != null) {
          out.write("<td width=\"10%\" align=\"right\">" + df.format(diskInfo.getCapacity()/1024/1024) + "</td>");
          out.write("<td width=\"10%\" align=\"right\">" + df.format(diskInfo.getUsed()/1024/1024) + "</td>");
          out.write("<td width=\"10%\" align=\"right\">" + df.format(serverInfo.getLogDirUsed()/1024/1024) + "</td>");
          out.write("<td width=\"10%\" align=\"right\">" + df.format(diskInfo.getAvailable()/1024/1024) + "</td>");
          out.write("<td width=\"10%\" align=\"right\">" + diskInfo.getPercentUsed() + " %</td>");
        } else {
          out.write("<td colspan=5>error get disk info.</td>");
        }
      } else {
        out.write("<td colspan=\"6\">&nbsp;</td>"); 
      }
      out.write("</tr>");
    }
    out.write("</table>");
    
    Set<String> failedCommitLogServers = CloudataMaster.cloudataMaster.getDeadCommitLogServers();
    if(!failedCommitLogServers.isEmpty()) {
      out.write("<h3>Dead CommitLogServers: " + failedCommitLogServers.size() + "</h3>");
      out.write("<table border=\"1\" width=\"100%\">");
      out.write("<tr><th>Node</th></tr>");
      for(String eachServer: failedCommitLogServers) {
        out.write("<tr><td>" + eachServer +"</td></tr>");
      }
      out.write("</table>");
    }    
  }
  
  public void getTabletServers(Writer out) throws IOException {
    TabletServerInfo[] tabletServers = CloudataMaster.cloudataMaster.getTabletServerInfos();
    if(tabletServers != null) {
      Arrays.sort(tabletServers);
    }
    
    if(tabletServers != null) {
      out.write("<h3>Live TabletServers: " + tabletServers.length + "</h3>");
      out.write("<table border=\"1\" width=\"100%\">");
      out.write("<tr><th>Node</th><th>Last<br/>Contact</th><th>Tablets</th></tr>");
      for(TabletServerInfo eachServer: tabletServers) {
        String hostName = eachServer.getHostName();
//        String status = "ok";
//        try {
//          DataServiceProtocol tabletServer = (DataServiceProtocol) NRPC.getProxy(DataServiceProtocol.class, 
//              DataServiceProtocol.versionID, 
//              NetworkUtil.getAddress(hostName), conf);  
//          tabletServer.test(0, "");
//        } catch (Exception e) {
//          status = "timeout";
//        }
        out.write("<tr>");
        out.write("<td><a href=\"tabletserver.jsp?tabletserver=" + hostName + "\">" + hostName + "</a></td>");
        out.write("<td>" + (System.currentTimeMillis() - eachServer.getLastHeartbeatTime())/1000 + "</td>");
        out.write("<td>" + eachServer.getNumOfTablets() + "</td>");
        out.write("</tr>");
      }    
      out.write("</table>");
    } else {
      out.write("<h3>Live TabletServers: 0</h3>");
    }
    
    Set<String> failedTabletServers = CloudataMaster.cloudataMaster.getDeadTabletServers();
    if(!failedTabletServers.isEmpty()) {
      out.write("<h3>Dead TabletServers: " + failedTabletServers.size() + "</h3>");
      out.write("<table border=\"1\" width=\"100%\">");
      out.write("<tr><th>Node</th></tr>");
      for(String eachServer: failedTabletServers) {
        out.write("<tr><td>" + eachServer +"</td></tr>");
      }
      out.write("</table>");
    }
  }
  
  public void getTables(Writer out) throws IOException {
    TableSchema[] tableSchemas = CloudataMaster.cloudataMaster.listTables();
    if(tableSchemas != null) {
      Arrays.sort(tableSchemas);
    }
    
    Map<String, Integer[]>  tabletNums = CloudataMaster.cloudataMaster.getNumTablets();
    if(tableSchemas != null) {
      out.write("<h3>Table: " + tableSchemas.length + "</h3>");
      out.write("<table border=\"1\" width=\"1000\">");
      out.write("<tr><th rowspan=\"2\">Table Name</th><th rowspan=\"2\">Owner</th><th rowspan=\"2\"># version</th><th colspan=\"3\">Tablet</th></tr>");
      out.write("<tr><th width=\"100\">total</th><th width=\"100\">unassigned</th><th width=\"100\">assigning</th>");
      for(TableSchema eachTable: tableSchemas) {
        String tableName = eachTable.getTableName();
        out.write("<tr>");
        out.write("<td><a href=\"table.jsp?table=" + tableName + "\">" + tableName + "</a></td>");
        out.write("<td>" + eachTable.getOwner() + "</td>");
        out.write("<td>" + eachTable.getNumOfVersion() + "</td>");
        
        Integer[] nums = tabletNums.get(tableName);
        if(nums != null) {
          out.write("<td align=\"right\">" + nums[0] + "</td><td align=\"right\">" + nums[1] + "</td><td align=\"right\">" + nums[2] + "</td>");
        } else {
          out.write("<td align=\"right\">0</td><td align=\"right\">0</td><td align=\"right\">0</td>");
        }
        out.write("</tr>");
      }    
      out.write("</table>");
    } else {
      out.write("<h3>Table: 0</h3>");
    }
  }
  
  
  public void getTabletDetailInfo(Writer out, String tabletName, String tabletServerHost) throws IOException {
    if(tabletServerHost == null) {
      out.write("Wrong tablet server");
      return;
    }

    TabletManagerProtocol tabletServer = (TabletManagerProtocol) CRPC.getProxy(TabletManagerProtocol.class, 
        TabletManagerProtocol.versionID, 
        NetworkUtil.getAddress(tabletServerHost), conf);
    
    TabletServerStatus tabletServerStatus = tabletServer.getServerStatus();

    out.write("<h1>" + tabletName + "(" + tabletServerHost + ")</h1>");
    out.write("<hr/>");
    out.write("<b>Tablet Server Summary</b>");
    out.write("<table border=\"0\">");
    out.write("<tr><td width=\"100\"><b>Started:</b></td><td>" + (new Date(tabletServerStatus.getServerStartTime())) + "</td></tr>");
    
    long free = tabletServerStatus.getFreeMemory();
    long max = tabletServerStatus.getMaxMemory();
    long total = tabletServerStatus.getTotalMemory();
    
    String threadLoad = "MajorCompaction Task: " + tabletServerStatus.getNumMajorThread() + ", " +
    "Split Task: " + tabletServerStatus.getNumSplitThread() + ", " +
    "Etc Task: " + tabletServerStatus.getNumEtcThread();
    
    String memory = "Free: " + df.format(free/1024) + " KB, " +
    		            "Total: " + df.format(total/1024) + " KB, " +
    		            "Max: " + df.format(max/1024) + " KB, " +
    		            "Cahce: " + df.format(tabletServerStatus.getMemoryCacheSize()/1024) + " KB";
    
    out.write("<tr><td width=\"100\"><b>Memory:</b></td><td>" + memory + "</td></tr>");
    out.write("<tr><td width=\"100\"><b>Load:</b></td><td>" + threadLoad + "</td></tr>");
    out.write("</table>");

    out.write("<hr/>");
    out.write("<b>Tablet</b><p/>");
    TabletReport tabletReport = tabletServer.getTabletDetailInfo(tabletName);
    
    if(tabletReport == null) {
      out.write("No tablet info: " + tabletName);
    }
    tabletReport.print(out);
    
    out.write("<hr/>");
    //out.write("<a href=\"tablet_memory.jsp?tabletName=" + tabletName + "\">show memory data</a><p>");
    out.write("<a href=\"javascript:alert('not yet');\">show memory data</a><p>");
    out.write("<hr/>");
    out.write("Map Files<p>");
    
    String[] mapFilePaths = tabletReport.getMapFilePaths();
    if(mapFilePaths == null || mapFilePaths.length == 0) {
      out.write("<ul>");
      out.write("<li>No map file</li>");
      out.write("</ul>");
    } else {
      for(String eachPath: mapFilePaths) {
        out.write("<ul>");
        out.write("<li>" + eachPath + "</li>");
        out.write("</ul>");
      }
    }
  }
  
  public void getCommitLogServerDetailInfos(Writer out, String server) throws IOException {
    out.write("<h1>CommitLogServer status: " + server + "</h1>");
    ServerMonitorInfo serverInfo = new ServerMonitorInfo();
    try {
      CommitLogServerIF commitLogServer = (CommitLogServerIF) CRPC.getProxy(CommitLogServerIF.class, 
          CommitLogServerIF.versionID, NetworkUtil.getAddress(server), conf);  
      serverInfo = commitLogServer.getServerMonitorInfo();
    } catch(Exception e) {
      out.write("<h1>Error</h1><pre>");
      e.printStackTrace(new PrintWriter(out));
      out.write("</pre>");
      return;
    }
    
    DiskInfo diskInfo = serverInfo.getDiskInfo();
    
    out.write("<table border=\"0\">");
    out.write("<tr><td width=\"100\"><b>Started:</b></td><td>" + (new Date(serverInfo.getServerStartTime())) + "</td></tr>");
    
    long free = serverInfo.getFreeMemory();
    long max = serverInfo.getMaxMemory();
    long total = serverInfo.getTotalMemory();
    
    String memory = "Free: " + df.format(free/1024) + " KB, Total: " + df.format(total/1024) + " KB, Max: " + df.format(max/1024) + " KB";

    out.write("<tr><td width=\"100\"><b>Heap:</b></td><td>" + memory + "</td></tr>");
    out.write("<tr><td width=\"100\"><b>Log path:</b></td><td>" + serverInfo.getLogPath() + " </td></tr>");
    out.write("<tr><td width=\"100\"><b>Capacity:</b></td><td>" + df.format(diskInfo.getCapacity()/1024/1024) + " MB</td></tr>");
    out.write("<tr><td width=\"100\"><b>Used:</b></td><td>" + df.format(diskInfo.getUsed()/1024/1024) + " MB</td></tr>");
    out.write("<tr><td width=\"100\"><b>Cloudata Used:</b></td><td>" + df.format(serverInfo.getLogDirUsed()/1024/1024) + " MB</td></tr>");
    out.write("<tr><td width=\"100\"><b>Available:</b></td><td>" + df.format(diskInfo.getAvailable()/1024/1024) + " MB</td></tr>");
    out.write("<tr><td width=\"100\"><b>Use:</b></td><td>" + diskInfo.getPercentUsed() + " %</td></tr>");
    
    out.write("</table>");
    out.write("<hr/>");
    
    List<String> logFiles = serverInfo.getLogFiles();
    Collections.sort(logFiles);

    out.write("<h3>Log files: " + logFiles.size() + "</h3>");
    
    if(logFiles.size() > 0) {
      out.write("<table border=\"1\" width=\"600\">");
      out.write("<tr><th>Log File</tr>");
      for(String eachFile: logFiles) {
        out.write("<tr><td>" + eachFile + "</td></tr>");
      }
      out.write("</table>");
    }
  }
  
  public void getTabletServerDetailInfos(Writer out, String tabletServerHost) throws IOException {
    if(tabletServerHost == null) {
      out.write("Wrong tablet server");
      return;
    }
    
    TabletManagerProtocol tabletServer = (TabletManagerProtocol) CRPC.getProxy(TabletManagerProtocol.class, 
        TabletManagerProtocol.versionID, 
        NetworkUtil.getAddress(tabletServerHost), conf);
    
    TabletServerStatus tabletServerStatus = tabletServer.getServerStatus();
    
    out.write("<h1>" + tabletServerHost + "</h1>");
    out.write("<table border=\"0\">");
    out.write("<tr><td width=\"100\"><b>Started:</b></td><td>" + (new Date(tabletServerStatus.getServerStartTime())) + "</td></tr>");
    
    long free = tabletServerStatus.getFreeMemory();
    long max = tabletServerStatus.getMaxMemory();
    long total = tabletServerStatus.getTotalMemory();
    
    String memory = "Free: " + df.format(free/1024) + " KB, " +
    		            "Total: " + df.format(total/1024) + " KB, " +
    		            "Max: " + df.format(max/1024) + " KB, " +
    		            "Cache: " + df.format(tabletServerStatus.getMemoryCacheSize()/1024) + " KB";
    
    out.write("<tr><td width=\"100\"><b>Memory:</b></td><td>" + memory + "</td></tr>");
    
    String threadLoad = "MajorCompaction Task: " + tabletServerStatus.getNumMajorThread() + ", " +
                        "Split Task: " + tabletServerStatus.getNumSplitThread() + ", " +
                        "MinorCompaction Task: " + tabletServerStatus.getNumEtcThread() + ", " +
                        "CommitLog pipe: " + tabletServerStatus.getCommitLogPipe();
    out.write("<tr><td width=\"100\"><b>Load:</b></td><td>" + threadLoad + "</td></tr>");
    out.write("</table>");
    out.write("<hr/>");
    
    TabletInfo[] tabletInfos = tabletServer.reportTablets();
    if(tabletInfos != null) {
      Arrays.sort(tabletInfos);
    }

    if(tabletInfos != null) {
      out.write("<h3>Tablet: " + tabletInfos.length + "</h3>");
      printTabletReport(out, tabletInfos, 1, tabletInfos.length);
      
    } else {
      out.write("<h3>Tablet: 0</h3>");
    }
    out.write("<hr/>");
    out.write("<h3>Tx Statistics</h3><a href=\"javascript:showComments();\">comments</a>");
    out.write("<div id=\"comments\" style=\"display:none;position:absolute;width:750px;height:60px; border:#000099 solid 1px; padding:5px; color:#000099; background-color:#FFFFFF;\">");
    out.write("<ul>");
    out.write("<li>[put: &lt;sum put time&gt;/&lt;# put operation&gt; = &lt;avg. put time&gt; (&lt;median put time&gt;) &lt;total put bytes&gt;],...</li>");
    out.write("<li>put retry: # retry put operation, cause by change log writing error</li>");
    out.write("<li>put skip: # skiped put operation, cause by TabletServer overloaded memory or tablet spliting, but automatically retried by NTable</li>");
    out.write("</ul>");
    out.write("</div>");
    out.write("<p/>");
    out.write("<table border=\"1\" width=\"800\">");
    for(String eachItem: tabletServerStatus.getTxStatistics()) {
      out.write("<tr><td>" + eachItem + "</td></tr>");
    }
    out.write("</table>");    
  }
  
  private String getPageMove(String url, String paramName, String paramValue, int recordTotalCount, int page, int maxOfPage) throws IOException {
    int startNum = (page - 1) * maxOfPage;
    int endNum   = page * maxOfPage;
    
    boolean next = true;
    boolean previous = true;
    if(endNum >= recordTotalCount) {
      endNum = recordTotalCount;
      next = false;
    }
    
    if(page == 1) {
      previous = false;
    }

    String result = "<table border=\"0\" width=\"100%\"><tr><td width=\"50%\">";
    if(previous) {
      result += ("<a href=\"" + url + "?" + paramName + "=" + paramValue + "&page=" + (page - 1) + "\">[Previous]</a>&nbsp;&nbsp;");
    }
    if(next) {
      result += ("<a href=\"" + url + "?" + paramName + "=" + paramValue + "&page=" + (page + 1) + "\">[Next]</a>");
    }
    
    result += "</td><td width=\"50%\" align=\"right\"><a href=\"/status.jsp\">[Goto Main]</a></td></tr></table>";
    return result;
  }
  
  private void printTabletReport(Writer out, TabletInfo[] tabletInfos, int page, int maxOfPage) throws IOException {
    int errorCount = 0;
    long totalMapFileSize = 0;
    long totalIndexFileSize = 0;
    long totalMemorySize = 0;
    
    int startNum = (page - 1) * maxOfPage;
    int endNum   = page * maxOfPage;
    if(endNum >= tabletInfos.length) {
      endNum = tabletInfos.length;
    }
    
    int count = startNum + 1;
    
    out.write("<table border=\"1\" width=\"100%\">");
    out.write("<tr><th width=\"5%\">No</th><th width=\"5%\">Table Name</th><th width=\"10%\">Tablet Name</th><th width=\"10%\">host</th><th width=\"20%\">end row</th><th width=\"50%\">Tablet summary</th></tr>");
    
    StringBuilder sb = new StringBuilder(1000);
    for(int i = startNum ;i < endNum; i++) {
      sb.setLength(0);
      TabletInfo eachTabletInfo = tabletInfos[i];
      try {
        if(eachTabletInfo.getAssignedHostName() == null) {
          sb.append("<tr>");
          sb.append("<td colspan=\"6\">Assigned host name is null" + eachTabletInfo.getTabletName() + "</td>");
          sb.append("</tr>");
          out.write(sb.toString());
          errorCount++;
          continue;
        }
        TabletManagerProtocol tabletServer = (TabletManagerProtocol) CRPC.getProxy(TabletManagerProtocol.class, 
            TabletManagerProtocol.versionID, 
            NetworkUtil.getAddress(eachTabletInfo.getAssignedHostName()), conf);
        
        TabletReport tabletReport = tabletServer.getTabletDetailInfo(eachTabletInfo.getTabletName());
        if(tabletReport == null) {
          sb.append("<tr>");
          sb.append("<td colspan=\"6\">Error reporting TabletInfo:" + eachTabletInfo.getTabletName() + "</td>");
          sb.append("</tr>");
          errorCount++;
          out.write(sb.toString());
          continue;
        }
        String tableName = eachTabletInfo.getTableName();
        String tabletName = eachTabletInfo.getTabletName();
        Row.Key endRowKey = eachTabletInfo.getEndRowKey();
        String endRowKeyStr = new String(endRowKey.getBytes(), "EUC-KR");
        if(endRowKey.equals(Row.Key.MAX_KEY)) {
          endRowKeyStr = "MAX";
        }
        if(endRowKeyStr.length() > 100) {
          endRowKeyStr = endRowKeyStr.substring(0, 100) + "...";
        }
        
        sb.append("<tr>");
        sb.append("<td>" + count + "</td>");
        sb.append("<td>" + tableName + "</td>");
        sb.append("<td><a href=\"tablet.jsp?tablet=" + tabletName + "&tabletserver=" + eachTabletInfo.getAssignedHostName() + "\">" + tabletName + "</a></td>");
        sb.append("<td>" + eachTabletInfo.getAssignedHostName() + "</td>");
        sb.append("<td>" + endRowKeyStr + "</td>");
        sb.append("<td><nobr>");
        
        out.write(sb.toString());
        tabletReport.print(out);
        out.write("</nobr></td>");
        out.write("</tr>");
        totalMapFileSize += tabletReport.sumMapFileSize();
        totalIndexFileSize += tabletReport.sumMapFileIndexSize();
        totalMemorySize += tabletReport.getMemoryTabletSize();
        count++;
      } catch (Exception e) {
        sb.append("<td colspan=\"6\">Error reporting TabletInfo:" + eachTabletInfo.getTabletName() + "</td>");
        out.write(sb.toString());
      }
    }
    out.write("</table>");
    out.write("# of Tablets:" + tabletInfos.length + ", Total MapFile size: " + df.format(totalMapFileSize/1024) + " KB, " +
        "Index File size: " + df.format(totalIndexFileSize/1024) + " KB, " +
    		"MemoryTablet: " + df.format(totalMemorySize/1024) + " KB, Report error: " + errorCount);
  }
  public void getTableInfo(Writer out, String tableName, int page) throws IOException {
    if(tableName == null) {
      out.write("<h3>Invalid table name</h3>");
      return;
    }
    out.write("<h3>Table: " + tableName + "</h3><p>");
    CTable ctable = CTable.openTable(conf, tableName);
    TableSchema schema = ctable.descTable();
    
    String columnStr = "[";
    for(ColumnInfo eachColumn: schema.getColumnInfoArray()) {
      columnStr += eachColumn.toString() + ",";
    }
    out.write(columnStr.substring(0, columnStr.getBytes().length - 1) + "]");
    out.write("<hr>"); 
    
    TabletInfo[] listTabletInfos = ctable.listTabletInfos();
    if(listTabletInfos == null) {
      out.write("No Tablets");
      return;
    }
    out.write(getPageMove("/table.jsp", "table", tableName, listTabletInfos.length, page, 1000));
    printTabletReport(out, listTabletInfos, page, 1000);
  }

  public Row[] runQuery(String tableName, String[] columns, String startRowKey, 
      String endRowKey, String operation, String paramAllVersion) throws Exception {
    if(tableName == null || tableName.length() == 0 || columns == null || columns.length == 0 ||
        startRowKey == null || startRowKey.trim().length() == 0) {
      return null;
    }
    CTable ctable = CTable.openTable(conf, tableName);
    if(endRowKey == null || endRowKey.trim().length() == 0) {
      endRowKey = startRowKey;
    }
    
    RowFilter rowFilter = new RowFilter(new Row.Key(startRowKey), 
        new Row.Key(endRowKey),
        Integer.parseInt(operation));

    boolean allVersion = "Y".equals(paramAllVersion);
    
    for(String eachColumn: columns) {
      CellFilter cellFilter = new CellFilter(eachColumn);
      if(allVersion) {
        cellFilter.setTimestamp(Long.MIN_VALUE, Long.MAX_VALUE);
      }
      rowFilter.addCellFilter(cellFilter);
    }
    
    return ctable.gets(rowFilter);
  }
  
  public void showQueryCondition(Writer out, String selectedTableName, String[] selectedColumns, String startRowKey, 
      String endRowKey, String paramOperation, String paramAllVersion) throws Exception {
    TableSchema[] tableSchemas = CloudataMaster.cloudataMaster.listTables();
    if(tableSchemas == null) {
      return;
    }
    
    if(tableSchemas != null) {
      Arrays.sort(tableSchemas);
    }
    
    out.write("<table border=\"1\" width=\"100%\">");
    out.write("<tr><td width=33% valign=top>");
    
    //show tables
    TableSchema selectedTableSchema = null;
    if(tableSchemas != null) {
      out.write("<table border=\"0\" width=\"100%\">\n");
      out.write("<tr><th align=left>Table Name</th></tr>\n");
      for(TableSchema eachTable: tableSchemas) {
        String eachTableName = eachTable.getTableName();
        String bgColor = "#ffffff";
        if(selectedTableName != null && eachTableName.equals(selectedTableName)) {
          bgColor = "#c5beb9";
          selectedTableSchema = eachTable;
        }
        out.write("<tr><td bgColor=\"" + bgColor + "\"><a href=\"query.jsp?tableName=" + eachTableName + "\">" + eachTableName + "</a></td></tr>\n");
      }    
      out.write("</table>\n");
    } 
    out.write("</td><td width=33% valign=top>\n");
    
    Set<String> columnNameSet = new HashSet<String>();
    if(selectedColumns != null) {
      for(String eachColumn: selectedColumns) {
        columnNameSet.add(eachColumn);
      }
    }
    //show columns
    out.write("<table border=\"0\" width=\"100%\">\n");
    out.write("<tr><th align=left>Column Name</th></tr>\n");
    if(selectedTableSchema != null) {
      for(ColumnInfo eachColumn: selectedTableSchema.getColumnInfos()) {
        String checked = "";
        if(columnNameSet.contains(eachColumn.getColumnName())) {
          checked = "checked";
        }
        out.write("<tr><td><input type=\"checkbox\" name=\"columnName\" " + checked + " value=\"" + eachColumn.getColumnName() + "\">" + eachColumn.toString() + "</td></tr>\n");
      }    
    } 
    out.write("</table>\n");
    out.write("</td><td width=33% valign=top>\n");
    
    //rowkey
    if(startRowKey == null) {
      startRowKey = "";
    }
    if(endRowKey == null) {
      endRowKey = "";
    }
    
    String[] opNames = new String[]{"=","like","between"};
    int operation = 0;
    if(paramOperation != null) {
      operation = Integer.parseInt(paramOperation);
    }
    
    String allVersion = "";
    if("Y".equals(paramAllVersion)) {
      allVersion = "checked";
    }
    
    out.write("Start RowKey:<input type=\"text\" size=\"30\" name=\"startRowKey\" value=\"" + startRowKey + "\"><p>\n");
    out.write("End RowKey:<input type=\"text\" size=\"30\" name=\"endRowKey\" value=\"" + endRowKey + "\"><p>\n");
    out.write("Operation:");
    for(int i = 0; i < opNames.length; i++) {
      String checked = (i == operation ? "checked" : "");
      out.write("<input type=\"radio\" name=\"operation\" " + checked + " value=\"" + i + "\"/> " + opNames[i] + "&nbsp;&nbsp;\n");
    }
    out.write("<p><input type=\"checkbox\" name=\"allVersion\" " + allVersion + " value=\"Y\"> all version<p>\n");
    out.write("<input type=\"submit\" value=\"query\" style=\"width:90px\"><p>"); 
    out.write("</td></tr></table>\n");
  }
  
  public List<MethodInfo> getRpcMethods(String type) throws IOException {
    List<MethodInfo> result = new ArrayList<MethodInfo>();
    if("master".equals(type)) {
      addMethod(result, TableManagerProtocol.class.getMethods());
      addMethod(result, TabletMasterProtocol.class.getMethods());
    } else if("tabletserver".equals(type)) {
      addMethod(result, TabletManagerProtocol.class.getMethods());
      addMethod(result, DataServiceProtocol.class.getMethods());
    } else if("commitlog".equals(type)) {
      addMethod(result, CommitLogServerIF.class.getMethods());
    }    
    return result;
  }
  
  private void addMethod(List<MethodInfo>result, Method[] methods) {
    for(Method eachMethod: methods) {
      boolean ok = true;
      String signature = eachMethod.getReturnType().getSimpleName() + " " +
                    eachMethod.getName() + "(";
      
      Class[] params = eachMethod.getParameterTypes();
      
      if(params != null && params.length > 0) {
        for(Class eachParam: params) {
          String paramName = eachParam.getSimpleName();
          if(!"String".equals(paramName) &&
              !"int".equals(paramName) &&
              !"long".equals(paramName) &&
              !"boolean".equals(paramName) &&
              !"Key".equals(paramName) ) {
            ok = false;
            break;
          }
          signature += eachParam.getSimpleName() + ", ";
        }
        signature = signature.substring(0, signature.length() - 2);
      } 
       
      signature += ")";
      if(ok) {
        result.add(new MethodInfo(signature, eachMethod));
      }
    }
  }
  
  public static class MethodInfo {
    String signature;
    Method method;
    
    public MethodInfo(String signature, Method method) {
      this.signature = signature;
      this.method = method;
    }
    
    public String toString() {
      return signature;
    }

    public String getSignature() {
      return signature;
    }

    public Method getMethod() {
      return method;
    }
    
    public String getParamInputForm() {
      String result = ""; 
      Class[] params = method.getParameterTypes();
     
      if(params != null && params.length > 0) {
        for(Class eachParam: params) {
          String paramName = eachParam.getSimpleName();
          result += paramName + ": <input type=\"text\" size=\"10\"><br>";
        }
      }
      
      return result;
    }
  }
  
  public static void main(String[] args) throws IOException {
    JspHelper jspHelper = new JspHelper();
    List<MethodInfo> datas = jspHelper.getRpcMethods("tabletserver");
    
    for(MethodInfo eachData: datas) {
      System.out.println(">>>" + eachData);
    }
  }
}
