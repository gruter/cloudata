/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.cloudata.core.client.shell;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.Shell;
import org.cloudata.core.client.cql.statement.ExecStatus;
import org.cloudata.core.client.cql.statement.RowIterator;
import org.cloudata.core.client.cql.statement.SelectStatement;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.util.StringUtils;
import org.cloudata.core.tablet.TabletInfo;


/**
 * @author jindolk
 *
 */
public class SelectShellStatement extends ShellStatement {
  public static final Log LOG = LogFactory.getLog(SelectShellStatement.class.getName());

  public static final String RECORD_DELIM = " | ";

  public static final String LINE_CONER = " + ";

  public static final String ROW_NUM = "#";
  
  private final int MAX_COLUMN_LENGTH = 1000;

  private int[] columnsFieldLength;

  private int printRowKeyLength = 6;

  private DecimalFormat df = new DecimalFormat("0000000000");
  
  private SelectStatement statement;
  
  public SelectShellStatement() {
  }
  
  public SelectShellStatement(SelectStatement statement) {
    this.statement = statement;
  }
  @Override
  public ExecStatus execute(CloudataConf conf) {
    ExecStatus status = statement.execute(conf);
    
    if(status.isError()) {
      return status;
    }
    try {
      int count = printResult(status.getRowIterator());
      
      status.setMessage(count + " rows selected.");
    } catch (Exception e) {
      status.setException(e);
    }
    return status;
  }
  
  private int printResult(RowIterator rowIterator)throws Exception  {
    if(rowIterator == null) {
      return 0;
    }
    String[] columns = rowIterator.getColumns();
    columnsFieldLength = new int[columns.length];
    for (int i = 0; i < columns.length; i++) {
      columnsFieldLength[i] = columns[i].length();
    } 
    
    boolean printHeader = false;
    int count = 0;

    int printRowKeyCount = 0;

    Row row = null;
    while ((row = rowIterator.nextRow()) != null) {
      Row.Key rowKey = row.getKey();
      int maxColumnCount = 0;
      for (int i = 0; i < columns.length; i++) {
        List<Cell> cells = row.getCellList(columns[i]);
        if (cells != null) {
          if (cells.size() > maxColumnCount) {
            maxColumnCount = cells.size();
          }
        }
      }

      count++;

      String[] printValues = new String[columns.length];
      for (int i = 0; i < maxColumnCount; i++) {
        for (int j = 0; j < columns.length; j++) {
          List<Cell> cells = row.getCellList(columns[j]);
          if (cells == null) {
            printValues[j] = "";
          } else {
            if (cells.size() <= i) {
              printValues[j] = "";
            } else {
              if(Constants.META_COLUMN_NAME_TABLETINFO.equals(columns[j])) {
                TabletInfo tabletInfo = new TabletInfo();
                tabletInfo.readFields(cells.get(0).getBytes());
                printValues[j] = tabletInfo.toString() + "," + cells.get(0).getValue().isDeleted();
              } else {
                printValues[j] = cells.get(i).getPrintValue(Shell.encoding);
              }
              if (printValues[j].getBytes().length > MAX_COLUMN_LENGTH) {
                printValues[j] = StringUtils.substring(printValues[j], 0, MAX_COLUMN_LENGTH);
              }
            }
          }
        }

        if (!printHeader) {
          for (int j = 0; j < columns.length; j++) {
            if (printValues[j].getBytes().length > columnsFieldLength[j]) {
              columnsFieldLength[j] = printValues[j].getBytes().length;
            }
          }
          if (rowKey.toString().getBytes().length > this.printRowKeyLength && rowKey.toString().getBytes().length < MAX_COLUMN_LENGTH) {
            printRowKeyLength = rowKey.toString().getBytes().length;
          }

          printHead(columns);
          printHeader = true;
        }

        printLine(count, rowKey.toString(), printValues);

        if (i == 0) {
          rowKey = Row.Key.MIN_KEY;
        }
      }
      printRowKeyCount++;
    } // end of while

    if (!printHeader) {
      printHead(columns);
      printHeader = true;
    }
    printHeadLine(columns);
    rowIterator.close();

    return count;
  }

  private void printHead(String[] columns) {
    printHeadLine(columns);
    System.out.print(RECORD_DELIM);

    System.out.printf("%-10s", ROW_NUM);
    System.out.print(RECORD_DELIM);
    
    System.out.printf("%-" + printRowKeyLength + "s", "Row.Key");
    System.out.print(RECORD_DELIM);

    for (int i = 0; i < columns.length; i++) {
      String format = "%-" + columnsFieldLength[i] + "s";
      System.out.printf(format, columns[i]);
      System.out.print(RECORD_DELIM);
    }
    System.out.print("\n");
    printHeadLine(columns);
  }

  private void printHeadLine(String[] columns) {
    System.out.print(LINE_CONER);

    String line = StringUtils.fillString("-", 10);
    System.out.print(line);
    System.out.print(LINE_CONER);

    line = StringUtils.fillString("-", printRowKeyLength);
    System.out.print(line);
    System.out.print(LINE_CONER);
    
    for (int i = 0; i < columns.length; i++) {
      line = StringUtils.fillString("-", columnsFieldLength[i]);
      System.out.print(line);
      System.out.print(LINE_CONER);
    }
    System.out.print("\n");
  }

  private void printLine(long rownum, String rowKey, String[] values) {
    String format = "%-" + printRowKeyLength + "s";

    System.out.print(RECORD_DELIM);
    if(rownum == 0) {
      System.out.printf("%-10s", " ");
    } else {
      System.out.printf("%-10s", df.format(rownum));
    }
    System.out.print(RECORD_DELIM);

    if (rowKey.length() == 0) {
      System.out.printf(format, "");
      System.out.print(RECORD_DELIM);
    } else {
      System.out.printf(format, "[" + rowKey + "]");
      System.out.print(RECORD_DELIM);
    }

    for (int i = 0; i < values.length; i++) {
      format = "%-" + columnsFieldLength[i] + "s";
      if (values[i].length() == 0) {
        System.out.printf(format, " ");
        System.out.print(RECORD_DELIM);
      } else {
        //System.out.printf(format, StringUtils.substring(values[i], 0, columnsFieldLength[i]));
        System.out.printf(format, values[i]);
        System.out.print(RECORD_DELIM);
      }
    }
    System.out.print("\n");
  }
  
  @Override
  public HelpUsage getHelpUsage() {
    return null;
  }

  @Override
  public String getPrefix() {
    return null;
  }
}
