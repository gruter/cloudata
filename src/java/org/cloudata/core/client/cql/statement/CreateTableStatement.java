package org.cloudata.core.client.cql.statement;

import java.io.IOException;

import org.cloudata.core.client.CTable;
import org.cloudata.core.client.cql.statement.ExecStatus;
import org.cloudata.core.client.cql.statement.QueryStatement;
import org.cloudata.core.client.shell.HelpUsage;
import org.cloudata.core.client.shell.StatementFactory;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.tablet.TableSchema;


public class CreateTableStatement implements QueryStatement {
  private TableSchema table;

  public void setTable(TableSchema table) {
    this.table = table;
  }

  @Override
  public ExecStatus execute(CloudataConf conf) {
    ExecStatus status = new ExecStatus();
    try {
      CTable.createTable(conf, table);
      status.setMessage(table.getTableName() + " table created.");
    } catch (IOException e) {
      status.setException(e);
    }
    return status;
  }

  @Override
  public String getQuery(CloudataConf conf) {
    String result = "CREATE TABLE " + table.getTableName() + "(";
    
    String columns = "";
    for(String eachColumn: table.getColumns()) {
      columns += eachColumn + ", ";
    }
    if(columns.length() > 0) {
      columns = columns.substring(0, columns.length() - 2);
    }
    result += columns + ")";
    
    result += " VERSION = " + table.getNumOfVersion();
    
    if(table.getDescription() != null) {
      result += " COMMENT = '" + table.getDescription() + "'"; 
    }
    return result; 
  }
  
  @Override
  public String getPrefix() {
    return StatementFactory.CREATE_TABLE;
  }
  
  @Override
  public HelpUsage getHelpUsage() {
    return new HelpUsage(getPrefix(), "Create a table.", 
        "CREATE TABLE <table_name> \n" +
        "  (column_name1[, column_name2, ...]) [VERSION = <version>] [comment='<comment>'];");
  }
}
