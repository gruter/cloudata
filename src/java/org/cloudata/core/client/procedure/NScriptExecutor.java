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
package org.cloudata.core.client.procedure;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaFileObject;

/**
<PRE>
clickHistory = "select item from T_MUSIC_USER where rowkey='$0'"; 
double sumS = 0.0; 
double sumRS = 0.0; 
for(int i = 0; i < $clickHistory.item.length; i++) { 
  //System.out.println("1:" + $clickHistory.item[i].columnKey);

  cov = "select item from T_MUSIC_ITEM where rowkey='" + $clickHistory.item[i].columnKey + "' and item='$1'";
  
  if($cov.item != null && $cov.item.length > 0) {
    //System.out.println("2:" + new String($cov.item[0].value));
    sumS += new Double(new String($cov.item[0].value)); 
  } else {
    //System.out.println("2:empty");
  }
} 

System.out.println(">>>" + sumS);
</PRE>
 * @author babokim
 *
 */
public class NScriptExecutor {
  private static final String PACKAGE_NAME = "org.cloudata.core.client.procedure.runtime";
  private static final String SELECT_FOR_SCAN = "for scan";
  private static final String SPACE = "          ";
  private Random random = new Random();
  private int classNameSuffix = 0;
  private String template;
  
  public void exec(String scriptFileName, boolean printSource, String[] userArgs) {
    CharSequenceCompiler<NScript> compiler = new CharSequenceCompiler<NScript>(
        getClass().getClassLoader(), Arrays.asList(new String[] { "-target", "1.6" }));
    
    NScript nscript = null;
    String source = null;
    try {
      String expr = script2Java(loadScript(scriptFileName));
      
      if(userArgs != null && userArgs.length > 0) {
        for(int i = 0; i < userArgs.length; i++) {
          expr = expr.replace("$" + i, userArgs[i]);
        }
      }
      // generate semi-secure unique package and class names
      final String packageName = PACKAGE_NAME + digits();
      final String className = "ScriptGeneratedClass_" + (classNameSuffix++) + digits();
      final String qName = packageName + '.' + className;
      // generate the source class as String
      source = fillTemplate(packageName, className, expr);

      if(printSource) {
        printSource(source);
      }
      
      //System.exit(0);
      // compile the generated Java source
      final DiagnosticCollector<JavaFileObject> errs = new DiagnosticCollector<JavaFileObject>();
      Class<NScript> compiledFunction = compiler.compile(qName, source, errs,
            new Class<?>[] { NScript.class });
      nscript = compiledFunction.newInstance();
      
   } catch (CharSequenceCompilerException e) {
     final StringBuilder msgs = new StringBuilder();
     for (Diagnostic<? extends JavaFileObject> diagnostic : e.getDiagnostics().getDiagnostics()) {
        msgs.append(diagnostic.getMessage(null)).append("\n");
     }
     System.out.println(msgs.toString());
     printSource(source);
     return;
   } catch (Exception e) {
     e.printStackTrace();
     printSource(source);
     return;
   }
   
   try {
     nscript.execute();
   } catch (NScriptException e) {
     e.printStackTrace();
   }
  }
  
  private void printSource(String expr) {
    System.out.println("============ Generated Source ==============");
    BufferedReader reader = new BufferedReader(new StringReader(expr));
    
    int lineNum = 1;
    String line = null;
    try {
      while( (line = reader.readLine()) != null ) {
        System.out.printf("%4d\t%s\n", lineNum, line);
        lineNum++;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    System.out.println("=============================================");
  }
  
  private String fillTemplate(String packageName, String className, String expression) throws IOException {
    if (template == null) {
      template = readTemplate();
    }
    String source = template.replace("$packageName", packageName)//
         .replace("$className", className)//
         .replace("$expression", expression);
    return source;
  }
  
  private String readTemplate() throws IOException {
    InputStream is = NScriptExecutor.class.getResourceAsStream("NScript.java.template");
    int size = is.available();
    byte bytes[] = new byte[size];
    if (size != is.read(bytes, 0, size))
       throw new IOException();
    return new String(bytes);
  }
  
  private String loadScript(String fileName) throws IOException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));
    StringBuilder sbuilder = new StringBuilder();
    
    String line = null;
    while( (line = reader.readLine()) != null ) {
      sbuilder.append(line).append("\n");
    }
    
    return sbuilder.toString();
 }
  
  private String digits() {
    return '_' + Long.toHexString(random.nextLong());    
  }
  
  private String script2Java(String expr) throws IOException {
    BufferedReader reader = new BufferedReader(new StringReader(expr));
    
    String line = null;
    int lineCount = 0;
    List<QueryItem> queryItems = new ArrayList<QueryItem>();
    
    while( (line = reader.readLine()) != null ) {
      lineCount++;
      if(line.trim().startsWith("//")) {
        continue;
      }
      
      if(line.indexOf("\"select") >= 0) {
        boolean selectForScan= false;
        
        int selectIndex = line.indexOf("\"select");
        String queryStr = line.substring(selectIndex + 1);
        int index = queryStr.indexOf(";");
        if(index < 0) {
          throw new IOException("Query Error [" + line + "]");
        }
        String sql = queryStr.substring(0, index);
        sql = sql.trim().substring(0, sql.getBytes().length - 1);
        index = sql.indexOf(SELECT_FOR_SCAN);
        if(index > 0) {
          sql = sql.substring(0, index);
          selectForScan = true;
        }
        
        index = line.indexOf("=");
        if(index < 0 || index >= selectIndex) {
          throw new IOException("Query must assigned variable [" + line + "]");
        }
        String variable = line.substring(0, index).trim(); 
        
        queryItems.add(new QueryItem(variable, sql, lineCount, selectForScan));
      }
    }
    reader.close();
    
    String parsedExpr = expr;
    
    for(QueryItem eachQueryItem: queryItems) {
      Selector selector = new Selector(eachQueryItem.query); 
      String[] columns = selector.getColumns();

      String scriptVariable = "$" + eachQueryItem.variable;
      
      String replacedVariable = "_" + eachQueryItem.variable + "_";

      for(int i = 0; i < queryItems.size(); i++) {
        if(eachQueryItem.scan) {
          queryItems.get(i).query = replaceScannerColumnValue(scriptVariable, replacedVariable, queryItems.get(i).query);
        } else {
          queryItems.get(i).query = replaceSelectColumnValue(selector, columns, scriptVariable, replacedVariable, queryItems.get(i).query);
        }
      }
    }
    
    int addedLine = 0;
    for(QueryItem eachQueryItem: queryItems) {
      if(eachQueryItem.scan) {
        StringBuilder resultHolder = new StringBuilder();
        addedLine += replaceScannerVariable(parsedExpr, eachQueryItem.query, eachQueryItem.variable,
            eachQueryItem.fromLine + addedLine, resultHolder);
        
        parsedExpr = resultHolder.toString();
      } else {
        StringBuilder resultHolder = new StringBuilder();
        addedLine += replaceSelectVariable(parsedExpr, eachQueryItem.query, eachQueryItem.variable,
            eachQueryItem.fromLine + addedLine, resultHolder);
        
        parsedExpr = resultHolder.toString();
      }
    }
    
    return parsedExpr;
  }

  private int replaceSelectVariable(String expr, String query, String variable, int fromLine, StringBuilder resultHolder) throws IOException {
    Selector selector = new Selector(query); 
    String[] columns = selector.getColumns();

    String scriptVariable = "$" + variable;
    
    String replacedVariable = "_" + variable + "_";
    
    BufferedReader reader = new BufferedReader(new StringReader(expr));
    String line = null;
    int lineCount = 0;
    int addedLineCount = 0;
    while( (line = reader.readLine()) != null ) {
      lineCount++;
      //System.out.println(">>>>>" + lineCount + ">" + fromLine + ">" + line);
      if(lineCount < fromLine) {
        resultHolder.append(SPACE).append(line).append("\n");
      } else if(lineCount == fromLine) {
        //Selector _a_selector = new Selector(query);
        //ColumnValue[][] columnValues = selector.select();
        resultHolder.append(SPACE).append("Selector " + replacedVariable + "selector = new Selector(\"" + query + "\");\n");
        resultHolder.append(SPACE).append("ColumnValue[][] " + replacedVariable + "columnValues = " + replacedVariable + "selector.select(); \n");
        addedLineCount = 1;
      } else {
        line = replaceSelectColumnValue(selector, columns, scriptVariable, replacedVariable, line);
        resultHolder.append(SPACE).append(line).append("\n");
      }
    }
    reader.close();

    return addedLineCount;
  }

  private String replaceSelectColumnValue(Selector selector, String[] columns, String scriptVariable, String replacedVariable, String line) {
    //$a.col1.     -> _a_columnValues[0].
    for(String eachColumn: columns) {
      int index = selector.getColumnIndex(eachColumn);
      line = line.replace(scriptVariable + "." + eachColumn + ".", replacedVariable + "columnValues[" + index + "].");
    }

    //$a.col1[0].rowKey  -> _a_ColumnValues[0][0].getRow.KeyKey()
    for(int i = 0; i < columns.length; i++) {
      String scriptColumnVariable = scriptVariable + "." + columns[i] + "[";
      int index = selector.getColumnIndex(columns[i]);
      line = line.replace(scriptColumnVariable, replacedVariable + "columnValues[" + index + "][");
    }
//    line = line.replace("].rowKey", replacedVariable + "].getRowKey()");
//    line = line.replace("].columnKey", replacedVariable + "].getCell.Key()");
//    line = line.replace("].value", replacedVariable + "].getValue()");
//    line = line.replace("].timestamp", replacedVariable + "].getTimestamp()");
//    line = line.replace("].numOfVersion", replacedVariable + "].getNumOfVersion()");
    
    line = line.replace("].rowKey", "].getRowKey()");
    line = line.replace("].columnKey", "].getCell.Key()");
    line = line.replace("].value", "].getValue()");
    line = line.replace("].timestamp", "].getTimestamp()");
    line = line.replace("].numOfVersion", "].getNumOfVersion()");    
    
//  $a.col1     -> _a_columnValues[0]
    for(String eachColumn: columns) {
      int index = selector.getColumnIndex(eachColumn);
      line = line.replace(scriptVariable + "." + eachColumn, replacedVariable + "columnValues[" + index + "]");
    }
    
    return line;
  }
  
  private int replaceScannerVariable(String expr, String query, String variable, int fromLine, StringBuilder resultHolder) throws IOException {
    String scriptVariable = "$" + variable;
    
    String replacedVariable = "_" + variable + "_";
    
    BufferedReader reader = new BufferedReader(new StringReader(expr));
    String line = null;
    int lineCount = 0;
    int addedLineCount = 0;
    while( (line = reader.readLine()) != null ) {
      lineCount++;
      if(lineCount < fromLine) {
        resultHolder.append(SPACE).append(line).append("\n");
      } else if(lineCount == fromLine) {
        //Selector _a_selector = new Selector(query);
        resultHolder.append(SPACE).append("Selector " + replacedVariable + "selector = new Selector(\"" + query + "\");\n");
      } else {
        if(line.trim().indexOf("while") >= 0 && line.trim().indexOf(scriptVariable + ".next(") > 0) {
          //MultiTabletMultiColumnScanner _a = _a_selector.getScanner(conf, query);
          //RowColumnValues _a_scanValue = null;
          //while((_a_scanValue = _a.next()) != null) {
          //  ColumnValue _a_columnValue = _a_scanValue.getColumnRecords()[0];
          resultHolder.append(SPACE).append("MultiTabletMultiColumnScanner " + replacedVariable + " = " + replacedVariable + "selector.getScanner();\n");
          resultHolder.append(SPACE).append("RowColumnValues " + replacedVariable + "scanValue = null; \n");
          resultHolder.append(SPACE).append("while((" + replacedVariable + "scanValue = " + replacedVariable + ".next()) != null) { \n");
          resultHolder.append(SPACE).append("  ColumnValue " + replacedVariable + "columnValue = " + replacedVariable + "scanValue.getColumnRecords()[0]; \n");
          addedLineCount = 3;
        } else {
          //$a.rowKey       -> _a_ColumnValue.getRow.KeyKey()
          //$a.columnKey    -> _a_ColumnValue.getCell.Key()
          //$a.value        -> _a_ColumnValue.getValue()
          //$a.timestamp    -> _a_ColumnValue.getTimestamp()
          //$a.numOfVersion -> _a_ColumnValue.getNumOfVersion()
          line = replaceScannerColumnValue(scriptVariable, replacedVariable, line);
          resultHolder.append(SPACE).append(line).append("\n");
        }
      }
    }
    reader.close();
    
    return addedLineCount;
  }

  private String replaceScannerColumnValue(String scriptVariable, String replacedVariable, String line) {
    line = line.replace(scriptVariable + ".rowKey", replacedVariable + "columnValue.getRowKey()");
    line = line.replace(scriptVariable + ".columnKey", replacedVariable + "columnValue.getCell.Key()");
    line = line.replace(scriptVariable + ".value", replacedVariable + "columnValue.getValue()");
    line = line.replace(scriptVariable + ".timestamp", replacedVariable + "columnValue.getTimestamp()");
    line = line.replace(scriptVariable + ".numOfVersion", replacedVariable + "columnValue.getNumOfVersion()");
    return line;
  }
  
  public static void main(String[] args) {
    args = new String[]{"d:/temp/sample.nql", "1728ydk", "1577875"};
    
    if(args.length < 1) {
      System.out.println("Usage: java NScriptExecutor <filename>");
      return;
    }
    
    String[] userArgs = null;
    if(args.length > 1) {
      userArgs = new String[args.length - 1];
      System.arraycopy(args, 1, userArgs, 0, userArgs.length);
    }
    
    (new NScriptExecutor()).exec(args[0], true, userArgs);
  }
  
  static class QueryItem {
    String query;
    String variable;
    int fromLine;
    boolean scan;
    
    public QueryItem(String variable, String query, int fromLine, boolean scan) {
      this.query = query;
      this.variable = variable;
      this.fromLine = fromLine;
      this.scan = scan;
    }
  }
  
  public void test() {
  }
}
