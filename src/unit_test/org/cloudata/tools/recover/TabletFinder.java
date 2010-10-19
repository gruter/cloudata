package org.cloudata.tools.recover;

import java.io.IOException;

import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.conf.CloudataConf;


public class TabletFinder {
  public static void main(String[] args) throws IOException {
    if(args.length < 2) {
      System.out.println("Usage java TabletFinder <table_name> <key>");
      System.exit(0);
    }
    CTable ctable = CTable.openTable(new CloudataConf(), args[0]);
    System.out.println(ctable.lookupTargetTablet(new Row.Key(args[1])));
  }
}

