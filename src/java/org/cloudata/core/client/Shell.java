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
package org.cloudata.core.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.cloudata.core.client.cql.javacc.CqlParser;
import org.cloudata.core.client.cql.javacc.ParseException;
import org.cloudata.core.client.cql.statement.ExecStatus;
import org.cloudata.core.client.cql.statement.QueryStatement;
import org.cloudata.core.client.cql.statement.SelectStatement;
import org.cloudata.core.client.procedure.NScriptExecutor;
import org.cloudata.core.client.shell.SelectShellStatement;
import org.cloudata.core.client.shell.StatementFactory;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.tablet.CloudataDataVerifier;
import org.cloudata.core.tablet.TableSchema;
import org.cloudata.core.tablet.backup.BackupBinaryJob;
import org.cloudata.core.tablet.backup.BackupJob;
import org.cloudata.core.tablet.backup.RestoreJob;
import org.cloudata.util.upload.UploadUtil;

import jline.ConsoleReader;



/**
 * Cloudata shell.
 */

public class Shell {
  public static final boolean DEFAULT_BELL_ENABLED = true;
  public static String encoding = "utf-8";
  
  public static CloudataConf conf = new CloudataConf();
  
  public static void main(String[] args) throws Exception {
    List<String> params = new ArrayList<String>();
    
    if(args.length > 0) {
      for(int i = 0; i < args.length; i++) {
        if("-h".equals(args[i])) {
          printUsage();
          System.exit(0);
        } else if("-charset".equals(args[i])) {
          i++;
          Shell.encoding = args[i];          
        } else {
          params.add(args[i]);
        }
      }
    } 
    
    args = params.toArray(new String[params.size()]);
    
    if(args.length > 0) {
      Shell shell = new Shell();
      for(int i = 0; i < args.length; i++) {
        if("-q".equals(args[i])) {
          i++;
          shell.executeQuery(args[i]);
          break;
        } else if("-file".equals(args[i])) {
          i++;
          String[] userArgs = new String[args.length - i];
          System.arraycopy(args, i, userArgs, 0, userArgs.length);
          shell.executeFile(userArgs);
          break;
        } else if("-check".equals(args[i])) {
          i++;
          String[] userArgs = new String[args.length - i];
          System.arraycopy(args, i, userArgs, 0, userArgs.length);
          (new CloudataDataVerifier(userArgs)).runCheck();
        } else if("-upload".equals(args[i])) {
          i++;
          String[] userArgs = new String[args.length - i];
          System.arraycopy(args, i, userArgs, 0, userArgs.length);
          (new UploadUtil()).doUpload(conf, userArgs);
        } else if("-scan".equals(args[i])) {
          i++;
          String[] userArgs = new String[args.length - i];
          System.arraycopy(args, i, userArgs, 0, userArgs.length);
          //TODO scan 기능 추가
        } else if("-backup".equals(args[i])) {
          i++;
          String[] userArgs = new String[args.length - i];
          System.arraycopy(args, i, userArgs, 0, userArgs.length);
          if(userArgs.length < 2) {
            printUsage();
            return;
          } else if(userArgs.length >= 2) {
            if("Y".equals(userArgs[0].trim().toUpperCase())) {
              (new BackupBinaryJob()).runBackUp(userArgs[1], userArgs[2]); 
            } else if("N".equals(userArgs[0].trim().toUpperCase())) {
              (new BackupJob()).runBackUp(userArgs[1], userArgs[2]); 
            } else {
              printUsage();
            }
          }
        } else if("-restore".equals(args[i])) {
          i++;
          String[] userArgs = new String[args.length - i];
          System.arraycopy(args, i, userArgs, 0, userArgs.length);
          if(userArgs.length < 5) {
            printUsage();
            return;
          } else if(userArgs.length >= 5) {
            if("Y".equals(userArgs[2].trim().toUpperCase())) {
              //(new BackupBinaryJob()).runBackUp(userArgs[0], userArgs[1]); 
            } else if("N".equals(userArgs[0].trim().toUpperCase())) {
              String[] columnNames = userArgs[2].split(",");
              (new RestoreJob()).runRestore(userArgs[1], columnNames, Integer.parseInt(userArgs[3]), userArgs[4]);
            } else {
              printUsage();
            }
          }
        } else {
          printUsage();
          System.exit(0);
        }
      }
    } else {
      Shell shell = new Shell();
      shell.runShell();
    }
  }
  
  private static void printUsage() {
    System.out.println("Usage: java Shell [-charset <charset>][<option(-q|-f)> <params>]");
    System.out.println("         -q <query> : run query directly");
    System.out.println("         -file <nql file> [-print] [input params] : run nql file");
    System.out.println("         -check <params> : verify ROOT, META, data file or index file");
    System.out.println("         -scan <params> : scan table's data");
    System.out.println("         -backup <binary(Y|N)> <table name> <output path>");
    System.out.println("         -restore <binary(Y|N)> <table name> <columnName1,columnName2,...> <numOfVersion> <input path> ");
  }
  
  public void runShell() throws Exception {
    ConsoleReader console = new ConsoleReader();
    
    printShellVersion();
    StringBuffer sb = new StringBuffer();
    String line;
    while ((line = console.readLine(getPrompt(sb))) != null) {
      sb.append(" " + line);
      
      if (isEndOfQuery(line)) {
        executeQuery(sb.toString().trim());
        sb = new StringBuffer();
      }
    }
    System.out.println();
  }

  public void printShellVersion() {
    System.out.println("Cloudata Shell(ver " + Constants.MAJOR_VERSION + "." + 
        Constants.MINOR_VERSION + "." + Constants.MINOR_VERSION_2 + ")\n" +
        "Type 'help;' for usage.\n");
  }
  
  private void executeFile(String[] args) throws IOException {
    if(args.length == 0) {
      printUsage();
      return;
    }
    
    List<String> argsList = new ArrayList<String>();
    boolean printSounrce = false;
    
    if(args.length > 1) {
      for(int i = 1; i < args.length; i++) {
        if("-print".equals(args[i])) {
          printSounrce = true;
        } else {
          argsList.add(args[i]);
        }
      }
    }
    String[] userArgs = new String[argsList.size()];
    argsList.toArray(userArgs);
    
    (new NScriptExecutor()).exec(args[0], printSounrce, userArgs);
  }
  
  private void executeQuery(String query) throws IOException {
    if(query.endsWith(";")) {
      query = query.substring(0, query.getBytes().length - 1);
    }
    QueryStatement statement = null;

    String uppercase = query.toUpperCase();
    if(uppercase.startsWith(StatementFactory.CREATE_TABLE) ||
        uppercase.startsWith(StatementFactory.DROP_TABLE) ||
        uppercase.startsWith(StatementFactory.INSERT) ||
        uppercase.startsWith(StatementFactory.SELECT) ||
        uppercase.startsWith(StatementFactory.DELETE)) {
      CqlParser parser = new CqlParser(query);
      
      try {
        statement = parser.parseStmt();
        if(statement instanceof SelectStatement) {
          statement = new SelectShellStatement((SelectStatement)statement);
        }
        
      } catch (ParseException pe) {
        System.out.println("Syntax error : Type 'help' for usage");
        pe.printStackTrace();
        return;
      }
    } else {
      statement = StatementFactory.getStatement(query);
      if(statement == null) {
        System.out.println("Syntax error : Type 'help' for usage");
        return;
      }
    }
    long startTime = System.currentTimeMillis();
    ExecStatus execStatus = statement.execute(Shell.conf);
    long endTime = System.currentTimeMillis();
    
    if(execStatus != null) {
      execStatus.printMessage(endTime - startTime);
    }
  }
  
  boolean isEndOfQuery(String line) {
    return (line.lastIndexOf(';') > -1) ? true : false;
  }

  private String getPrompt(final StringBuffer sb) {
    return (sb.toString().equals("")) ? "Cloudata> " : "     -> ";
  }
  
  public static void printHead(String name) {
    System.out.println("+------+----------------------+-----------------------------------------+");
    System.out.print("| No.  | ");
    System.out.printf("%-20s", name);
    System.out.println(" |");
  }

  public static void printFoot() {
    System.out.println("+------+----------------------+-----------------------------------------+");
    System.out.println();
  }

  public static void printTable(int count, TableSchema tableSchema) {
    String tableName = tableSchema.getTableName();
    String tableDetail = tableSchema.getTableDetail();
    
    System.out.println("+------+----------------------+-----------------------------------------+");
    System.out.print("| ");
    System.out.printf("%-4s", count + 1);
    System.out.print(" | ");
    System.out.printf("%-20s", tableName);
    System.out.print(" |");
    System.out.printf("%-40s", tableDetail);
    System.out.println(" |");
  } 
}
