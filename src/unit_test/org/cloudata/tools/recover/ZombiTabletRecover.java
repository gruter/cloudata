package org.cloudata.tools.recover;

import java.io.BufferedReader;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.cloudata.core.client.CellFilter;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.RowFilter;
import org.cloudata.core.client.scanner.ScannerFactory;
import org.cloudata.core.client.scanner.TableScanner;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.tablet.TabletInfo;


/**
 * Tablet이 분리되면 다음과 같이 되어야 한다.
 * A[1-10] -> a1[1-5], a2[5-10]
 * 어떤 문제로 인해 A[1-10], a[5-10] 모두 존재하는 경우
 * 범위가 작은 Tablet을 dump 받은 후 범위가 작은 tablet 정보를 삭제한다.
 * 그리고 dump 받은 데이터를 로딩한다.
 * @author babokim
 *
 */
public class ZombiTabletRecover {
  //bin/cloudata jar cloudata-1.4.0-test.jar org.cloudata.tools.recover.ZombiTabletRecover ./dump_target.dat /data2/cloudata_dump > dump.result &
  static CloudataConf conf = new CloudataConf();
  static String[] columns = new String[]{"TWEETDATA",  
    "TWEETDATAREPLY", 
    "TWEETDATALINK", 
    "TWEETDATAMENTION", 
    "TWEETDATAHASH", 
    "TWEETDATAIMG", 
    "TWEETDATAGEO"}; 

  static String path;
  
  public static void main(String[] args) throws Exception {
    //getErrorTablets();
    if(args.length < 2) {
      System.out.println("Usage java ZombiTabletRecover <in> <out>");
      System.exit(0);
    }
    path = args[1];
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(new FileInputStream(args[0])));
    
    String line = null;
    int count = 0;
    List<String> tablets = new ArrayList<String>();
    
    while( (line = reader.readLine()) != null ) {
      count++;
      String[] datas = line.split(" ");
      if(datas.length == 0) {
        continue;
      }
      //System.out.println("[" + datas[0].trim() + "][" + datas[1].trim() + "]");
      tablets.add(datas[1].trim());
    }
    
    int numOfThread = 3;
    int numOfTabletsPerThread = tablets.size() / numOfThread;
    List<String> threadTablets = new ArrayList<String>();
    
    count = 0;
    for(int i = 0; i < tablets.size(); i++) {
      threadTablets.add(tablets.get(i));
      count++;
      
      if(count >= numOfTabletsPerThread) {
        DataDumper dumper = new DataDumper(threadTablets);
        dumper.start();
        threadTablets.clear();
        count = 0;
      } 
    }
    if(threadTablets.size() > 0) {
      DataDumper dumper = new DataDumper(threadTablets);
      dumper.start();
    }
    
    reader.close();
  }
  
  static class DataDumper extends Thread {
    List<String> tablets = new ArrayList<String>();
    public DataDumper(List<String> tablets) {
      this.tablets.addAll(tablets);
    }
    public void run() {
      String currentTablet = "";
      CTable ctable;
      try {
        ctable = CTable.openTable(conf, "TWITTER_DATA");
      } catch (IOException e1) {
        e1.printStackTrace();
        return;
      }
      for(String eachTablet: tablets) {
        try {
          currentTablet = eachTablet;
          System.out.println("Start " + eachTablet);
          TabletInfo tabletInfo = ctable.getTabletInfo(eachTablet);
          TableScanner scanner = ScannerFactory.openScanner(conf, tabletInfo, columns);
          
          FileOutputStream out = new FileOutputStream(path + "/" + eachTablet);
          DataOutput dout = new DataOutputStream(out);
          Row row = null;
          int count = 0;
          while( (row = scanner.nextRow()) != null ) {
            row.write(dout);
            count++;
          }
          out.close();
          scanner.close();
          System.out.println("End " + eachTablet + "," + count);
        } catch (Exception e) {
          System.out.println("Error " + currentTablet + "," + e.getMessage());
          e.printStackTrace();
        }
      }
    }
  }
  
  public static List<TabletInfo[]> getErrorTablets() throws Exception {
    List<TabletInfo[]> result = new ArrayList<TabletInfo[]>();
    CTable ctable = CTable.openTable(conf, "META");
    
    RowFilter rowFilter = new RowFilter();
    rowFilter.addCellFilter(new CellFilter("TabletInfo"));
    TableScanner scanner = ScannerFactory.openScanner(ctable, rowFilter);
    
    Row row = null;
    
    TabletInfo previousTabletInfo = null;
    while( (row = scanner.nextRow()) != null ) {
      TabletInfo tabletInfo = new TabletInfo();
      tabletInfo.readFields(row.getFirst("TabletInfo").getBytes());
      
      if(previousTabletInfo != null) {
        if(previousTabletInfo.getTableName().equals(tabletInfo.getTableName())) {
          if(!previousTabletInfo.getEndRowKey().equals(tabletInfo.getStartRowKey())) {
            //System.out.println(previousTabletInfo + "," + tabletInfo);
            if(previousTabletInfo.getStartRowKey().equals(tabletInfo.getStartRowKey())) {
              System.out.println("Type1:" + previousTabletInfo + "," + tabletInfo);
              //dump previous, delete previous
            } else {
              System.out.println("Type2:" + previousTabletInfo + "," + tabletInfo);
            }
            
            //result.add(new TabletInfo[]{previousTabletInfo, tabletInfo});
          }
        }
      }
      
      previousTabletInfo = tabletInfo;
    }
    
    return result;
  }
}

