package org.cloudata.core.commitlog;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.util.List;

import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.tablet.TabletInfo;
import org.cloudata.core.tabletserver.Tablet;


public class RecoverWithCommitLog {
    static String outputPath;
    static String commitLogPath;

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Usage: java RecoverWithMapFile <tablet list file> <commitlog path> <output path>");
            System.exit(0);
        }
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(args[0])));
        
        commitLogPath = args[1];
        outputPath = args[2];

        String line = "";

        while ((line = reader.readLine()) != null) {
            exec(line);
        }
//        exec("TWITTER_USER_18868809429221738");
    }

    public static void exec(String tabletName) throws Exception {
        FileOutputStream fout = new FileOutputStream(outputPath + "/" + tabletName);
        String tableName = Tablet.getTableNameFromTabletName(tabletName);
        CloudataConf nconf = new CloudataConf();
        CTable ctable = CTable.openTable(nconf, tableName);
        
        File file = new File(commitLogPath + "/" + tabletName + "_PipeBasedCommitLog.57011");

        if (!file.exists() || file.length() == 0) {
            System.out.println(file + " not exists or empty");
            return;
        }
        
        System.out.println("Start: " + file);
        FileInputStream in = new FileInputStream(file);
        int count = 0;
        try {
            DataInputStream dis = new DataInputStream(in);

            List<TransactionData> txDataList = CommitLogClient.readTxDataFrom(dis);

            for (TransactionData data : txDataList) {
                for (int i = 0; i < data.logList.length; i++) {
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

                    String value = data.logList[i].getValue() == null ? "null" : new String(data.logList[i].getValue());
                      
                    /*
                    System.out.println(data.logList[i].getRowKey() + "," + data.logList[i].getOperation() + "," + 
                            data.logList[i].getColumnName() + "," +
                            data.logList[i].getCellKey() + "," +
                            //value + "," +
                            data.logList[i].getTimestamp());
                    */
                    Row row = new Row(new Row.Key(data.logList[i].getRowKey()));
                    row.addCell(data.logList[i].getColumnName(),
                            new Cell(data.logList[i].getCellKey(), data.logList[i].getValue(), data.logList[i].getTimestamp()));
                    ctable.put(row, false);   
                    count++;
                    
                    if(count % 1000 == 0) {
                        System.out.println(count + " inserted " + file + "," + 
                                data.logList[i].getRowKey() + "," + data.logList[i].getColumnName() + "," + data.logList[i].getCellKey());
                    }
                }
            }
        } finally {
            in.close();
            fout.close();
        }
        System.out.println("End: " + file);
    }
}

