package org.cloudata.tools.recover;

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


public class MetaDataVerifier {
  static CloudataConf conf = new CloudataConf();
  public static void main(String[] args) throws Exception {
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
            if(previousTabletInfo.getStartRowKey().equals(tabletInfo.getStartRowKey())) {
              System.out.println("Type1:" + previousTabletInfo + "," + tabletInfo);
            } else {
              System.out.println("Type2:" + previousTabletInfo + "," + tabletInfo);
            }
          }
        }
      }
      
      previousTabletInfo = tabletInfo;
    }
  }
}

