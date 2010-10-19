package org.cloudata.examples;

import java.io.IOException;
import java.util.Random;

import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.conf.CloudataConf;

public class Test {
  String tableName = "SampleTable";
  CloudataConf conf = new CloudataConf();
  
  public static void main(String[] args) throws IOException {
    for(int i = 0; i < 20; i++) {
      TestThread t = new TestThread("" + i , i % 2 == 0);
      t.start();
    }
  }
  
  static class TestThread extends Thread {
    String id;
    boolean makeAll;
    public TestThread(String id, boolean makeAll) {
      this.id = id;
    }
    
    public void run() {
      Random rand = new Random(System.currentTimeMillis());

      CloudataConf conf = new CloudataConf();
      
      CTable ctable = null;
      if(!makeAll) {
        try {
          ctable = CTable.openTable(conf, "SampleTable");
        } catch (IOException e1) {
          e1.printStackTrace();
        }
      }
      while(true) {
        if(makeAll) {
          try {
            ctable = CTable.openTable(conf, "SampleTable");
          } catch (IOException e1) {
            e1.printStackTrace();
            continue;
          }
        }
        Row row;
        try {
          row = ctable.get(new Row.Key("RowKey1"));
          System.out.println(id + ": " + row.getKey());
        } catch (IOException e1) {
          e1.printStackTrace();
        }
        try {
          //Thread.sleep(1 + rand.nextInt(3) * 1000);
          Thread.sleep(5 * 1000);
        } catch (InterruptedException e) {
        }
      }
    }
  }
}  
    
