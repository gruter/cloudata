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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.RowFilter;
import org.cloudata.core.client.ScanCell;
import org.cloudata.core.client.TabletLocationCache;
import org.cloudata.core.tablet.TabletInfo;


/**
 * Target Tablet: 테이블의 모든 Tablet <br>
 * Target Column: single column <br>
 * <b>주의사항: 오픈된 하나의 Scanner로 클라이언트에서 멀티 쓰레드에서 작업할 경우 정확한 동작을 보장하지 않는다.</b>
 * @author 김형준
 *
 */
class MultiTabletScanner extends AbstractTableScanner {
  private static final Log LOG = LogFactory.getLog(MultiTabletScanner.class.getName());
  
  private CTable ctable;
  private TabletInfo currentScanTablet;
  private SingleTabletScanner currentScanner;
  private Row.Key startRowKey;
  private Row.Key endRowKey;
  private boolean end = false;
  private int timeout;
  private RowFilter rowFilter;
  
  private List<SingleTabletScanner> scanners = new ArrayList<SingleTabletScanner>();
  
  protected MultiTabletScanner(CTable ctable, 
      RowFilter rowFilter,
      int timeout) throws IOException {
    if(rowFilter.getColumns() == null || rowFilter.getColumns().length > 1) {
      throw new IOException("MultiTabletScanner must have only one column");
    }
    
    this.ctable = ctable;
    this.columnName = rowFilter.getColumns()[0];
    if(rowFilter == null) {
      throw new IOException("rowFilter null");
    }
    this.rowFilter = rowFilter;
    this.startRowKey = rowFilter.getStartRowKey();
    this.endRowKey = rowFilter.getEndRowKey();
    this.timeout = timeout;
    
    openSingleTabletScanner(startRowKey, false);
  }
  
  private void openSingleTabletScanner(Row.Key rowKey, boolean great) throws IOException {
    Row.Key targetRowKey = null;
    if(great) {
      byte[] endKeyBytes = new byte[rowKey.getLength() + 1];
      System.arraycopy(rowKey.getBytes(), 0, endKeyBytes, 0, rowKey.getLength());
      
      endKeyBytes[rowKey.getLength()] = (byte)0xff;
        
      targetRowKey = new Row.Key(endKeyBytes);
    } else {
      targetRowKey = new Row.Key(rowKey);
    }
    
    long startTime = System.currentTimeMillis();
    Exception exception = null;
    
    while(true) {
      TabletInfo tabletInfo = null;
      try {
        tabletInfo = ctable.lookupTargetTablet(targetRowKey);
        if(tabletInfo != null) {
          if(currentScanTablet != null && 
              currentScanTablet.equals(tabletInfo)) {
            if(currentScanner != null) {
              currentScanner.close();
            }
            currentScanner = null;
            currentScanTablet = null;
            return;
          }
          
          currentScanner = (SingleTabletScanner)ScannerFactory.openScanner(ctable.getConf(), 
              targetRowKey,
              tabletInfo, rowFilter);
          scanners.add(currentScanner);
          currentScanTablet = currentScanner.tabletInfo;
          return;
        }
      } catch (IOException e) {
        exception = e;
        LOG.warn("error openSingleTabletScanner:" + e.getMessage() + ", but rerty", e);
        if(currentScanner != null) {
          currentScanner.close();
        }
      }
      
      try {
        Thread.sleep(1 * 1000);
      } catch (InterruptedException e) {
        return;
      }
    
      if(timeout > 0) {
        if(System.currentTimeMillis() - startTime > timeout * 1000) {
          LOG.error("Timeout while opening scanner: timeout=" + timeout + ", rowKey=" + rowKey + ", last exception=" + exception);
          IOException e = new IOException("Timeout while opening scanner: timeout=" + timeout + ", rowKey=" + rowKey + ", last exception=" + exception);
          if(exception != null) {
            e.initCause(exception);
          }
          throw e;
        }
      }
    } // end of while         
  }
  
  protected boolean isEnd() {
    return end;
  }

  protected void setEnd(boolean end) {
    this.end = end;
  }

  public ScanCell next() throws IOException {
    ScanCell scanCell = nextRecord();
    while(true) {
      if(scanCell != null) {
        Row.Key rowKey = scanCell.getRowKey();
        if(rowKey.compareTo(startRowKey) >= 0 && rowKey.compareTo(endRowKey) <= 0 ) {
          return scanCell;
        } else if(rowKey.compareTo(endRowKey) > 0) {
          return null;
        } else { 
          scanCell = nextRecord();
          LOG.debug("SKIP:" + rowKey + "," + rowKey.compareTo(startRowKey)  + "," + rowKey.compareTo(endRowKey));
        }
      } else {
	/*
        return null;
	*/
        if(currentScanTablet == null) {
          return null;
        }
        
        if(currentScanTablet.getEndRowKey().compareTo(endRowKey) >= 0) {
          return null;
        } else {
          scanCell = nextRecord();
        }	
      }
    }
  }

  private ScanCell nextRecord() throws IOException {
    if(currentScanTablet == null) {
      end = true;
      return null;
    }
    
    ScanCell scanCell = currentScanner.next();
    if(scanCell == null) {
      currentScanTablet = moveNextTablet();
      
      if(currentScanTablet == null) {
        return null;
      }
      
      try {
        scanCell = currentScanner.next();
      } catch (Exception e) {
	e.printStackTrace();
        if(currentScanner != null) {
          currentScanner.close();
        }
        currentScanner = null;
        currentScanTablet = null;
        return null;
      }
    }
    return scanCell;
  }
  
  /**
   * 커서를 다음 tablet으로 이동시킨다.
   * @return 이동된 Tablet 정보
   * @throws IOException
   */
  public TabletInfo moveNextTablet() throws IOException {
    if(currentScanner != null) {
      currentScanner.close();
      currentScanner = null;
    }
    
    //마지막인 경우
    if(Row.Key.MAX_KEY.equals(currentScanTablet.getEndRowKey())) {
      if(currentScanner != null) {
        currentScanner.close();
      }
      currentScanTablet = null;
      return null;
    } else {
      openSingleTabletScanner(currentScanTablet.getEndRowKey(), true);
      return currentScanTablet;
    }
  }
  
  public void close() throws IOException {
    if(currentScanner != null) {
      currentScanner.close();
      currentScanner = null;
    }
    
    for(SingleTabletScanner eachScanner: scanners) {
      if(eachScanner != null) {
        eachScanner.close();
      }
    }
  }

  @Override
  public String[] getColumnNames() {
    return new String[]{columnName};
  }
}
