package org.cloudata.tools.recover;

import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.tablet.TabletInfo;


public class MetaUpdator {
  public static void main(String[] args) throws Exception {
    if(args.length < 2) {
      System.out.println("Usage MetaUpdator <option(u|d)> <key> [startKey]");
      System.exit(0);
    }
    CloudataConf conf = new CloudataConf();
    
    CTable ctable = CTable.openTable(conf, "META");
    
    if("d".equals(args[0])) {
      ctable.removeRow(new Row.Key(args[1]));
    } else if("u".equals(args[0])) {
      Row row = ctable.get(new Row.Key(args[1]));
      if(row == null) {
        System.out.println("No data:" + args[1]);
        return;
      }
      
      Cell cell = row.getFirst("TabletInfo");
      
      TabletInfo tabletInfo = new TabletInfo();
      tabletInfo.readFields(row.getFirst("TabletInfo").getBytes());
      tabletInfo.setStartRowKey(new Row.Key(args[2]));
      
      cell.getValue().setBytes(tabletInfo.getWriteBytes());
      cell.getValue().setTimestamp(System.currentTimeMillis());
      Row updateRow = new Row(row.getKey());
      updateRow.addCell("TabletInfo", cell);
      ctable.put(row);
    } else {
      System.out.println("Wrong option:" + args[0]);
    }
  }
}

