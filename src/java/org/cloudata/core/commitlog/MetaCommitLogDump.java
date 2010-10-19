package org.cloudata.core.commitlog;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.scanner.ScannerFactory;
import org.cloudata.core.client.scanner.TableScanner;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.tablet.TabletInfo;


public class MetaCommitLogDump {

  public static void main(String[] args) throws IOException {
    CloudataConf conf = new CloudataConf();
    CTable metaTable = CTable.openTable(conf, Constants.TABLE_NAME_META);
    TableScanner metaScanner = ScannerFactory.openScanner(metaTable, Constants.META_COLUMN_NAME_TABLETINFO); 
    try {
      Row metaRow = null;
      while( (metaRow = metaScanner.nextRow()) != null ) {
        TabletInfo userTabletInfo = new TabletInfo();
        List<Cell> cells = metaRow.getCellList(Constants.META_COLUMN_NAME_TABLETINFO); 
        userTabletInfo.readFields(cells.get(cells.size() - 1).getBytes());
        System.out.println(metaRow.getKey() + "," + userTabletInfo);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  public static void test(String[] args) throws IOException {
    if (args.length < 1) {
      System.out
          .println("Usage: java MetaCommitLogDump <commit log file> [output file]");
      System.exit(0);
    }

    File file = new File(args[0]);

    PrintStream out = null;
    if (args.length == 1) {
      out = System.out;
    } else {
      out = new PrintStream(new FileOutputStream(args[1]));
    }

    if (!file.exists()) {
      System.out.println("not exist");
      System.exit(0);
    }
    FileInputStream in = new FileInputStream(file);
    DataInputStream dis = new DataInputStream(in);

    List<TransactionData> txDataList = CommitLogClient.readTxDataFrom(dis);

    for (TransactionData data : txDataList) {
      for (int i = 0; i < data.logList.length; i++) {
        out.println("\tOP : " + data.logList[i].getOperation());

        if (data.logList[i].getOperation() == Constants.LOG_OP_MODIFY_META) {
          TabletInfo targetTablet = null;
          DataInput dataIn = null;

          TabletInfo[] splitedTabletInfos = new TabletInfo[2];
          try {
            targetTablet = new TabletInfo();
            dataIn = new DataInputStream(new ByteArrayInputStream(
                data.logList[i].getValue()));
            targetTablet.readFields(dataIn);

            for (int j = 0; j < 2; j++) {
              splitedTabletInfos[j] = new TabletInfo();
              splitedTabletInfos[j].readFields(dataIn);
            }
          } catch (EOFException e) {
            // 1.3 version
            targetTablet = new TabletInfo();
            dataIn = new DataInputStream(new ByteArrayInputStream(
                data.logList[i].getValue()));
            targetTablet.readOldFields(dataIn);

            for (int j = 0; j < 2; j++) {
              splitedTabletInfos[j] = new TabletInfo();
              splitedTabletInfos[j].readOldFields(dataIn);
            }
          }
          System.out.print(targetTablet.toString() + "," + splitedTabletInfos[0].toString() + "," + splitedTabletInfos[1].toString());
        } else if (data.logList[i].getOperation() == Constants.LOG_OP_DELETE_COLUMN_VALUE) {
          out.print("Deleted:" + data.logList[i].getRowKey());
        } else {
          TabletInfo targetTablet = null;
          DataInputStream dataIn = null;
          try {
            targetTablet = new TabletInfo();
            dataIn = new DataInputStream(new ByteArrayInputStream(
                data.logList[i].getValue()));
            targetTablet.readFields(dataIn);
          } catch (EOFException e) {
            // 1.3 version
            targetTablet = new TabletInfo();
            dataIn = new DataInputStream(new ByteArrayInputStream(
                data.logList[i].getValue()));
            targetTablet.readOldFields(dataIn);
          }
          
          System.out.print(targetTablet.toString());
        }
      }
    }
  }
}

