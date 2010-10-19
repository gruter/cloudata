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
package org.cloudata.core.client.scanner;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.client.CellFilter;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.CTableManager;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.RowFilter;
import org.cloudata.core.client.ScanCell;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.io.CWritableUtils;
import org.cloudata.core.common.util.NetworkUtil;
import org.cloudata.core.tablet.ColumnValue;
import org.cloudata.core.tablet.TabletInfo;
import org.cloudata.core.tabletserver.DataServiceProtocol;


/**
 * get 메소드는 호출할 때 마다 네트워크 연결을 새로 만들어 연속된 데이터를 조회하는 경우 성능이 저하된다.
 * 또한 get 메소드는 특정 rowkey를 이미 알고 있는 경우에만 사용할 수 있다.
 * 특정 rowkey가 아닌 연속된 데이터를 조회하는 경우에 Scanner를 이용한다. Scanner는 open되면 close되기 전까지 TabletServer와 계속
 * 연결을 유지하고 있어 빠른 속도로 데이터를 조회할 수 있다.
 * (일정 시간동안 next 호출이 되지 않는 경우 서버에서 자동으로 scanner를 close 처리한다) 
 * SingleTableScanner는 클라이언트에서 사용하는 모든 Scanner의 기본 Scanner로 하나의 Tablet, 하나의 Column에 대해서만 Scan을 수행한다.
 * startRowKey로부터 scan을 시작한 다음 endRowKey 또는 Tablet의 마지막까지 scan한다. 
 * Command Line 상에서도 수행이 가능하다.(명령어: bin/cloudata org.cloudata.core.client.SingleTabletScanner <table name> <tablet name>)
 * @see MultiTabletScanner SingleTabletMultipleColumnScanner MultipleColumnScanner 
 * @author 김형준
 *
 */
class SingleTabletScanner extends AbstractTableScanner {
  private static final Log LOG = LogFactory.getLog(SingleTabletScanner.class.getName());
  private Socket socket = null;
  
  private int READ_TIMEOUT = 60 * 1000;
  private DataInputStream in;
  private boolean end = false;
  
  protected TabletInfo tabletInfo;

  private ColumnValue[] preFetchedResults;
  
  private int prefetchIndex = 0; 
  
  private RowFilter rowFilter;
  
  private Thread touchThread;
  
  private AtomicLong lastNextTime = new AtomicLong();
  
  private ColumnValue lastColumnValue;
  
  private boolean timeoutException = false;
  
  protected SingleTabletScanner(final CloudataConf conf, 
                  final TabletInfo tabletInfo, 
                  final RowFilter rowFilter) throws IOException {
    this.tabletInfo = tabletInfo;

    //ScanFilter 중에서 column에 해당하는 것만 남겨둔다.
    if(rowFilter.getCellFilters().size() == 0) {
      throw new IOException("No column in RowFilter");
    } 
    this.rowFilter = rowFilter;
    this.columnName = rowFilter.getColumns()[0];
    
    String hostName = tabletInfo.getAssignedHostName();
    int index = hostName.indexOf(":");
    String host = hostName.substring(0, index);
    
    InetSocketAddress targetAddr = null;
    try {
      DataServiceProtocol tabletServer = CTableManager.connectTabletServer(hostName, conf);
      String port = tabletServer.getTabletServerConf("tabletServer.scanner.port");
      targetAddr = NetworkUtil.getAddress(host + ":" + port);
    } catch(IOException e) {
      LOG.error("Can't connect to " + hostName + "," + e.getMessage());
      throw e;
    }
    
    try {
      socket = new Socket();
      socket.connect(targetAddr, READ_TIMEOUT);
      socket.setSoTimeout(READ_TIMEOUT);
  
      DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
      out.write(Constants.OPEN_SCANNER);
      CWritableUtils.writeString(out, tabletInfo.getTabletName());
      CWritableUtils.writeString(out, columnName);
      rowFilter.write(out);
      out.flush();
  
      in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
      byte successOpen = (byte)in.read();
      if(successOpen == Constants.SCANNER_OPEN_FAIL) {
        String errorMessage =  CWritableUtils.readString(in);
        in.close();
        throw new IOException(errorMessage);
      }
      
      String scannerId = CWritableUtils.readString(in);
      touchThread = new Thread(new LeaseTouchThread(conf, tabletInfo.getAssignedHostName(), scannerId));
      touchThread.setName("ScannerLeaseTouchThread_" + tabletInfo.getTabletName() + "_" + System.currentTimeMillis());
      touchThread.start();
    } catch (SocketException e) {
      this.close();
      //LOG.warn("Scan open error cause:" + e.getMessage() + ", tablet=" + tabletInfo);
      throw e;
    } catch (IOException e) {
      this.close();
      //LOG.warn("Scan open error cause:" + e.getMessage() + ", tablet=" + tabletInfo, e);
      throw e;
    } catch (Exception e) {
      //LOG.warn("Scan open error cause:" + e.getMessage() + ", tablet=" + tabletInfo, e);
      throw new IOException(e.getMessage(), e);
    }
  }
  
  void touch() {
    
  }
  
  public ScanCell next() throws IOException {
    if(timeoutException) {
      throw new IOException("Scanner Touch Thread stoped cause next() calling is so dealyed(120sec)");
    }
    if(end) {
      if(touchThread.isAlive()) {
        touchThread.interrupt();
      }
      return null;
    }

    lastNextTime.getAndSet(System.currentTimeMillis());
    
    if(preFetchedResults != null && preFetchedResults.length > prefetchIndex) {
      ColumnValue result =  preFetchedResults[prefetchIndex];
      prefetchIndex++;
      lastColumnValue = result;
      //System.out.println(lastColumnValue.getRowKey().toString());
      return lastColumnValue.copyToScanCell(columnName);
    } 
    
    int size = -1;
    try {
      size =  in.readInt();
      //System.out.println("Size:" + size);
    } catch (IOException e) {
      if(lastColumnValue != null) {
        LOG.debug("lastColumnValue:" + lastColumnValue + "," + tabletInfo.getTabletName());
      }
      in.close();
      if(touchThread.isAlive()) {
        touchThread.interrupt();
      }
      LOG.error("Scan Error:" + tabletInfo);
      throw e;
    }
    
    if(size == -1) {
      end = true;
      if(touchThread.isAlive()) {
        touchThread.interrupt();
      }
      return null;
    }
    
    if(size == 0) {
      if(touchThread.isAlive()) {
        touchThread.interrupt();
      }
      return null;
    }
    
    preFetchedResults = new ColumnValue[size];
    //int count = 0;
    for(int i = 0; i < size; i++) {
      preFetchedResults[i] = new ColumnValue();
      preFetchedResults[i].readFields(in);
      //count++;
    }
    //System.out.println("Count:" + count);
    prefetchIndex = 1;
    
    lastColumnValue = preFetchedResults[0];
    
    
    return preFetchedResults[0].copyToScanCell(columnName);
  }

  protected boolean isEnd() {
    return end;
  }

  protected void setEnd(boolean end) {
    this.end = end;
  }
  
  public void close() throws IOException {
    if(in != null) {
      in.close();
    }
    if(socket != null) {
      socket.close();
    }
    if(touchThread != null) {
      touchThread.interrupt();
    }
  }
  
  public boolean equals(Object obj) {
    if( !(obj instanceof SingleTabletScanner) ) {
      return false;
    }
    SingleTabletScanner scanner = (SingleTabletScanner)obj;
    return tabletInfo.equals(scanner.tabletInfo);
  }
  
  class LeaseTouchThread implements Runnable {
    
    String leaseId;
    CloudataConf conf;
    String hostName;
    public LeaseTouchThread(CloudataConf conf,
                              String hostName, 
                              String leaseId) {
      this.leaseId = leaseId; 
      this.conf = conf;
      this.hostName = hostName;
    }
    
    public void run() {
      try {
        try {
          Thread.sleep(5 * 1000);
        } catch (InterruptedException e) {
          //System.out.println(touchThread.getName() + "\tEnd1");
          return;
        }
        int retry = 0;
        DataServiceProtocol tabletServer = null;
        while(true) {
          try {
            tabletServer = CTableManager.connectTabletServer(hostName, conf);
            break;
          } catch (Exception e) {
            retry++;
            if(retry >= 5) {
              LOG.error("Scanner Touch Open Error:Can't connectTabletServer:" + hostName + "," + tabletInfo , e);
            }
            try {
              Thread.sleep(3 * 1000);
            } catch (InterruptedException err) {
              return;
            }
          }
        }
        
        while(true) {
          try {
            tabletServer.touch(leaseId);
          } catch (Exception e) {
            LOG.error("Scanner Touch Error:" + hostName + "," + e.getMessage());
          }
          try {
            Thread.sleep(10 * 1000);
          } catch (InterruptedException e) {
            //System.out.println(touchThread.getName() + "\tEnd2");
            return;
          }
          
          //next call이 계속 안되는 경우 warn만 알려준다.
          //next가 call이 안되는 것은 사용자 측에서 close()를 명시적으로 하지 않은 경우임
          //이 경우 LeaseTouchThread가 종료되지 않기 때문에 사용자 애플리케이션이 종료되지 않는다.
          //사용자는 여기서 나타내는 warn을 보고 프로그램에서 잘못된 점을 찾아 close() 시키도록 유도한다.
          if(System.currentTimeMillis() - lastNextTime.get() > 120 * 1000) {
            LOG.warn("not called scanner.next() for 120sec after last calling. " +
            		"please check scanner.close() explicitly in your program." +
            		"tabletName=" + tabletInfo.getTabletName() + ",columnName=" + columnName);
            lastNextTime.getAndSet(System.currentTimeMillis());
          }
        }
      } catch (Exception e) {
        LOG.error("Scanner Touch Open Error:" + hostName + "," + tabletInfo , e);
      }
    }
  }

  @Override
  public String[] getColumnNames() {
    return new String[]{columnName};
  }
  
  public static void main(String[] args) throws Exception {
    if(args.length < 3) {
      System.out.println("Usgae java SingleTabletScanner <table name> <tablet name> <col name>");
      System.exit(0);
    }
    CTable ctable = CTable.openTable(new CloudataConf(), args[0]);
    TabletInfo tabletInfo = ctable.getTabletInfo(args[1]);
    if(tabletInfo == null) {
      System.exit(0);
      System.out.println("No Tablet:" + args[1]);
    } else {
      System.out.println("Tablet:" + tabletInfo);
    }
    
    RowFilter rowFilter = new RowFilter();
    rowFilter.addCellFilter(new CellFilter(args[2]));
    SingleTabletScanner scanner = new SingleTabletScanner(ctable.getConf(), tabletInfo, rowFilter);
    
    Row row = null;
    while( (row = scanner.nextRow()) != null ) {
      System.out.println("scanCell:" + row.getKey());
    }
  }
}

