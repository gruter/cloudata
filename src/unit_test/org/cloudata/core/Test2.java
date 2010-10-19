package org.cloudata.core;

import java.io.IOException;
import java.util.List;

import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CellFilter;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.RowFilter;
import org.cloudata.core.common.conf.CloudataConf;


public class Test2 {
  public static void main(String[] args) throws Exception {
    CloudataConf conf = new CloudataConf();
    
    CTable ctable = CTable.openTable(conf, "TWITTER_DATA_APPEND_ORDER_CURRENT_2");
    
    //000000000041784
    //String preLastKey = "000000000041700";
    String preLastKey = "000000000038000";
    RowFilter rowFilter = getRowFilterForListing("TWEETLOG", preLastKey, null);
    CTable ctable2 = CTable.openTable(conf, "TWITTER_DATA");
    
    int count = 0;
    while(true) {
      rowFilter.setPagingInfo(10, new Row.Key(preLastKey));
      Row[] rows = ctable.gets(rowFilter);
      if(rows == null || rows.length == 0) {
        break;
      }
  
      for(Row eachRow: rows) {
        List<Cell> cells = eachRow.getFirstColumnCellList();//eachRow.getCellList("TWEETLOG");
        
        for(Cell eachCell: cells) {
          String rowKeyData = eachCell.getKey().toString();
          Row result = ctable2.get(new Row.Key(rowKeyData));
          count++;
          if(count % 1000 == 0) {
            System.out.println("==================" + eachRow.getKey() + "==================" );
          }
        }
        preLastKey = eachRow.getKey().toString();
      }
    }
  }
  
  private static RowFilter getRowFilterForListing(String columeName, String startKey, String endKey) throws IOException {
    Row.Key startRowKey =new Row.Key((startKey==null) ? Long.toString(Long.MIN_VALUE) : startKey) ; 
    Row.Key endRowKey =new Row.Key((endKey==null)? Long.toString(Long.MAX_VALUE) : endKey) ; 
    
    RowFilter rowFilter = new RowFilter(startRowKey, endRowKey, RowFilter.OP_BETWEEN);
    rowFilter.addCellFilter(new CellFilter(columeName));
    rowFilter.setPagingInfo(10, null);
    return rowFilter;
}
}

