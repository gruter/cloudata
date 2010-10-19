package org.cloudata.core.commitlog;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import org.cloudata.core.common.Constants;
import org.cloudata.core.tablet.TabletInfo;


public class CommitLogDump {

  public static void main(String[] args) throws IOException {
    if(args.length < 2) {
      System.out.println("Usage: java CommitLogDump <detail(Y|N)> <commit log file> [output file]");
      System.exit(0);
    }
    
    File file = new File(args[1]);
    
    PrintStream out = null;
    if(args.length == 2) {
      out = System.out;
    } else {
      out = new PrintStream(new FileOutputStream(args[2]));
    }
    
    if (file.exists()) {
      try {
        FileInputStream in = new FileInputStream(file);
        DataInputStream dis = new DataInputStream(in);

        List<TransactionData> txDataList = CommitLogClient.readTxDataFrom(dis);
        
        boolean detail = "Y".equals(args[0]);
        
        for(TransactionData data : txDataList) {
          if(detail) {
            out.println("Seq# : " + data.seq);
            out.println("tabletName : " + new String(data.tabletName));
            out.println("log count : " + data.logList.length);
            
            for(int i = 0; i < data.logList.length; i++) {
              out.println("\n\tLOG# : " + i);
  
              out.println("\tOP : " + data.logList[i].getOperation());
              
              if (data.logList[i].getOperation() == Constants.LOG_OP_MODIFY_META) {
                try {
                  System.out.println("Start");
                  DataInput in1 = new DataInputStream(new ByteArrayInputStream(data.logList[i].getValue()));
                  TabletInfo targetTablet = new TabletInfo();
                  targetTablet.readFields(in1);
                  System.out.println("End");
                } catch (EOFException e) {
                  DataInput in1 = new DataInputStream(new ByteArrayInputStream(data.logList[i].getValue()));
                  TabletInfo targetTablet = new TabletInfo();
                  targetTablet.readOldFields(in1);
                }
              }
              
              out.println("\tTableName : " + data.logList[i].getTableName());
              out.println("\tRowKey : " + data.logList[i].getRowKey());
              out.println("\tColumnName : " + data.logList[i].getColumnName());
              out.println("\tCellKey : " + data.logList[i].getCellKey());
              out.println("\tTimestamp : " + data.logList[i].getTimestamp());
            }
          } else {
            for(int i = 0; i < data.logList.length; i++) {
              String line = "Seq#: " + data.seq + ", ";
              line += (i + ", OP: " + data.logList[i].getOperation()  + ", " +
                  "RowKey: " + data.logList[i].getRowKey() + ", " +
                  "ColumnName: " + data.logList[i].getColumnName() + ", " +
                  "CellKey: " + data.logList[i].getCellKey());       
              
              out.println(line);
            }
          }
        }
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }
    } else {
      System.out.println("not exist");
    }
  }
}

