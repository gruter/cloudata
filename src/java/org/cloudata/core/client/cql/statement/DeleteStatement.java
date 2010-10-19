package org.cloudata.core.client.cql.statement;

import java.io.IOException;
import java.util.List;

import org.cloudata.core.client.CellFilter;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.RowFilter;
import org.cloudata.core.client.cql.element.ColumnElement;
import org.cloudata.core.client.cql.element.WhereExpression;
import org.cloudata.core.client.shell.HelpUsage;
import org.cloudata.core.client.shell.StatementFactory;
import org.cloudata.core.common.conf.CloudataConf;


/**
 * delete (col1, col2) from T_TEST where rowkey='111' 
 * <P>
 * delete (col1) from T_TEST where rowkey='111' and col1 = '222'
 * <P>
 * delete from T_TEST where rowkey='111' and col1 = '222'
 */
public class DeleteStatement implements QueryStatement {
  String tableName;

  List<ColumnElement> columns;

  private WhereExpression where;

  @Override
  public ExecStatus execute(CloudataConf conf) {
    ExecStatus status = new ExecStatus();
    if(where == null) {
      status.setException(new IOException("No where clause"));
      return status;
    }

    try {
      CTable ctable = CTable.openTable(conf, tableName);
      
      if(ctable == null) {
        throw new IOException("No table " + tableName);
      }
      
      RowFilter rowFilter = new RowFilter();
      for(ColumnElement eachElement: columns) {
        List<String> columnNames= eachElement.getColumnNames(ctable);
        for(String eachColumn: columnNames) {
          rowFilter.addCellFilter(new CellFilter(eachColumn));
        }
      }
      where.initFilter(rowFilter);
      
      if(rowFilter.getCellFilters().size() == 0) {
        ctable.removeRow(rowFilter.getRowKey(), System.currentTimeMillis());
      } else {
        Row.Key rowKey = rowFilter.getRowKey();
        for(CellFilter eachCellFilter: rowFilter.getCellFilters()) {
          ctable.remove(rowKey, eachCellFilter.getColumnName(), eachCellFilter.getTargetCellKey());
        }
      }
      
      status.setMessage("1 row removed.");
    } catch (IOException e) {
      status.setException(e);
    }
    return status;
    
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public void setWhere(WhereExpression where) {
    this.where = where;
  }

  @Override
  public String getQuery(CloudataConf conf) {
    String result = "DELETE ";
    if (columns != null && columns.size() > 0) {
      for (ColumnElement eachColumn : columns) {
        result += eachColumn.getQuery(conf) + ",";
      }
      result = result.substring(0, result.length() - 1);
    }
    result += " FROM " + tableName.toString();

    result += " WHERE " + where.getQuery(conf);
    return result;
  }

  public void setColumns(List<ColumnElement> columns) {
    this.columns = columns;
  }

  @Override
  public HelpUsage getHelpUsage() {
    return new HelpUsage(getPrefix(), "Delete cell or row in table.", 
        "DELETE * | ( (column_name1 [, column_name2, ...] ) \n" +
        "  FROM <table_name>" + "\n" +
        " WHERE rowkey='row_key' [and column_name1 = 'column_key' and ...];");    
  }

  @Override
  public String getPrefix() {
    return StatementFactory.DELETE;
  }
}
