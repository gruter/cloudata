package org.cloudata.tools.recover;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.conf.CloudataConf;


public class DumpDataLoader {
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
    if(args.length < 1) {
      System.out.println("Usage java DumpDataLoader <in>");
      System.exit(0);
    }
    path = args[0];
    
    ExecutorService executor = Executors.newFixedThreadPool(3);
    
    File file = new File(path);
    File[] dumpFiles = file.listFiles();
    
    for(File eachFile: dumpFiles) {
      executor.execute(new CloudataUploader(eachFile));
    }
    executor.shutdown();
  }    
 
  static class CloudataUploader extends Thread {
    File dumpFile;
    public CloudataUploader(File dumpFile) {
      this.dumpFile = dumpFile;
    }
    public void run() {
      try {
        System.out.println("Start " + dumpFile);
        CTable ctable = CTable.openTable(conf, "TWITTER_DATA");
        FileInputStream in = new FileInputStream(dumpFile);
        DataInput din = new DataInputStream(in);
        int count = 0;
        try {
          while(true) {
            Row row = new Row();
            row.readFields(din);
            ctable.put(row);
            count++;
          }
        } catch (EOFException oe) {
        }
        in.close();
        System.out.println("End " + dumpFile + "," + count);
      } catch (Exception e) {
        System.out.println("Error " + dumpFile);
        e.printStackTrace();
      }
    }
  }
}
