package org.cloudata.core;

import org.cloudata.core.client.CellFilter;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.RowFilter;
import org.cloudata.core.client.scanner.ScannerFactory;
import org.cloudata.core.client.scanner.TableScanner;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.tablet.TabletInfo;


public class Test3 {
  public static void main(String[] args) throws Exception {
    CloudataConf conf = new CloudataConf();
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
            System.out.println(previousTabletInfo + "," + tabletInfo);
          }
        }
      }
      
      previousTabletInfo = tabletInfo;
    }
  }
}

