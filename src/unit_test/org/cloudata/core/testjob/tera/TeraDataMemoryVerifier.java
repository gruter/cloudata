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
package org.cloudata.core.testjob.tera;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.cloudata.core.client.CTable;
import org.cloudata.core.client.CTableManager;
import org.cloudata.core.commitlog.CommitLogStatus;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.util.NetworkUtil;
import org.cloudata.core.fs.CommitLogLoader;
import org.cloudata.core.fs.CloudataFileSystem;
import org.cloudata.core.fs.GPath;
import org.cloudata.core.tablet.ColumnValue;
import org.cloudata.core.tablet.TabletInfo;
import org.cloudata.core.tabletserver.CommitLog;
import org.cloudata.core.tabletserver.DataServiceProtocol;


/**
 * @author jindolk
 *
 */
public class TeraDataMemoryVerifier {
  public static void main(String[] args) throws Exception {
    if(args.length < 3) {
      System.out.println("Usage: java TeraDataMemoryVerifier <table name> <column name>");
      System.exit(0);
    }
    
    String tableName = args[0];
    String columnName = args[1];
    
    CloudataConf conf = new CloudataConf();
    
    CTable ctable = CTable.openTable(conf, tableName);
    TabletInfo[] tabletInfos = ctable.listTabletInfos();
    if(tabletInfos == null || tabletInfos.length == 0) {
      System.out.println("No Tablet in " + tableName);
      System.exit(0);
    }

    String memoryKeyPath = "memoryKey.dat";
    BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(memoryKeyPath));
    try {
      for(TabletInfo eachTabletInfo: tabletInfos) {
        DataServiceProtocol tabletServer = CTableManager.connectTabletServer(
            eachTabletInfo.getAssignedHostName(), conf);
        ColumnValue[] columnValues = tabletServer.getAllMemoryValues(eachTabletInfo.getTabletName(), columnName);
        if(columnValues == null) {
          out.write((eachTabletInfo.getTabletName() + " null\n").getBytes());
          
        } else {
          for(ColumnValue columnValue: columnValues) {
            out.write((eachTabletInfo.getTabletName() + " " + columnValue.getRowKey() + "\n").getBytes());
          }
        }
      }
    } finally {
      out.close();
    }
    
    CloudataFileSystem fs = CloudataFileSystem.get(conf);
    
    String commitLogKeyPath = "ccommitLogKey.dat";
    out = new BufferedOutputStream(new FileOutputStream(commitLogKeyPath));
    try {
      for(TabletInfo eachTabletInfo: tabletInfos) {
        InetSocketAddress[] addresses = getCommitLogAddress(conf, fs, eachTabletInfo.getTabletName()); 
        if(addresses == null || addresses.length == 0) {
          out.write((eachTabletInfo.getTabletName() + " noLog1\n").getBytes());
          continue;
        }
        
        CommitLogLoader loader = new CommitLogLoader(conf, eachTabletInfo.getTabletName(), 
            new CommitLogStatus(), addresses);
        
        CommitLog commitLog = null;
        int count = 0;
        while( (commitLog = loader.nextCommitLog()) != null ) {
          out.write((eachTabletInfo.getTabletName() + " " + commitLog.getRowKey() + "\n").getBytes());
          count++;
        }
        if(count == 0) {
          out.write((eachTabletInfo.getTabletName() + " noLog2\n").getBytes());
        }
      }
    } finally {
      out.close();
    }      
    
    //compare 
    BufferedReader memoryKeyReader = new BufferedReader(new InputStreamReader(new FileInputStream(memoryKeyPath)));
    BufferedReader commitLogKeyReader = new BufferedReader(new InputStreamReader(new FileInputStream(commitLogKeyPath)));
    try {
      int count = 1;
      while(true) {
        String line1 = memoryKeyReader.readLine();
        String line2 = commitLogKeyReader.readLine();
        if(line1 == null && line2 == null) {
          break;
        }
          
        if(line1 != null && line2 == null) {
          System.out.println("Unmatched_A:" + count + " [" + line1 + "][" + line2 + "]");
          break;
        }
        if(line1 == null && line2 != null) {
          System.out.println("Unmatched_B:" + count + " [" + line1 + "][" + line2 + "]");
          break;
        }
        
        if(!line1.equals(line2)) {
          System.out.println("Unmatched_C:" + count + " [" + line1 + "][" + line2 + "]");
          break;
        }
        count++;
      }
      System.out.println( (count - 1) + " lines verified");
    } finally {
      memoryKeyReader.close();
      commitLogKeyReader.close();
    }    
  }
  
  static InetSocketAddress[] getCommitLogAddress(CloudataConf conf, CloudataFileSystem fs, 
      String tabletName) throws IOException {
    String logImagePath = conf.get("commitlog.image.dir", "/user/cloudata/commitlog");
    GPath tabletPath = new GPath(logImagePath, tabletName);
    if (fs.exists(tabletPath)) {
      BufferedReader br = null;
      try {
        br = new BufferedReader(new InputStreamReader(fs.open(tabletPath)));
        String line = null;
        List<InetSocketAddress> addrList = new ArrayList<InetSocketAddress>();
        while ((line = br.readLine()) != null) {
          addrList.add(NetworkUtil.getAddress(line));
        }

        if (!addrList.isEmpty()) {
          return addrList.toArray(new InetSocketAddress[0]);
        } else {
          return null;
        }
      } finally {
        if (br != null) {
          br.close();
        }
      }
    } else {
      return null;
    }
  }
}
