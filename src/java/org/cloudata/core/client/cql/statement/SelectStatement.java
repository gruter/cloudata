package org.cloudata.core.client.cql.statement;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.cloudata.core.client.CellFilter;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.RowFilter;
import org.cloudata.core.client.cql.element.ColumnElement;
import org.cloudata.core.client.cql.element.ExpressionElement;
import org.cloudata.core.client.cql.element.FromElement;
import org.cloudata.core.client.cql.element.WhereExpression;
import org.cloudata.core.client.cql.element.WhereExpressionElement;
import org.cloudata.core.client.scanner.ScannerFactory;
import org.cloudata.core.client.scanner.TableScanner;
import org.cloudata.core.client.shell.HelpUsage;
import org.cloudata.core.client.shell.StatementFactory;
import org.cloudata.core.common.conf.CloudataConf;


public class SelectStatement implements QueryStatement, ExpressionElement {
  private int top;
  private List<ColumnElement> selectColumns = new ArrayList<ColumnElement>();
  private List<FromElement> fromTables = new ArrayList<FromElement>();
  private WhereExpression where;
  
	public void setSelectColumns(List<ColumnElement> selectColumns) {
	  this.selectColumns = selectColumns;
	}

	public void setFromTables(List<FromElement> fromTables) {
	  this.fromTables = fromTables;
	}

	public void setWhere(WhereExpression where) {
	  this.where = where;
	}

	public void setTop(int top) {
	  this.top = top;
	}
	
	@Override
	public String getQuery(CloudataConf conf) {
	  String query = "SELECT ";
	  String columnQuery = "";
	  for(ColumnElement eachElement: selectColumns) {
	    columnQuery += eachElement.getQuery(conf) + ", ";
	  }
	  
	  if(columnQuery.length() > 1) {
	    columnQuery = columnQuery.substring(0, columnQuery.length() - 2);
	  }
	  
	  query += columnQuery;

	  String fromQuery = "";
    for(FromElement eachElement: fromTables) {
      fromQuery += eachElement.getQuery(conf)+ ", ";
    }
    
    if(fromQuery.length() > 1) {
      fromQuery = fromQuery.substring(0, fromQuery.length() - 2);
    }
	    
    query += " FROM " + fromQuery;
    
    if(where != null) {
      query += " WHERE " + where.getQuery(conf);
    }
	  return query;
	}

  @Override
  public ExecStatus execute(CloudataConf conf) {
    ExecStatus status = new ExecStatus();
    
    try { 
      //현재 버전에서는 from 절에 subquery나 여러개의 테이블이 나타나는 것을 지원하지 않음
      if(fromTables.size() > 1) {
        throw new IOException("Not support multi tables");
      }
      String tableName = fromTables.get(0).getTableName();
      
      CTable ctable = CTable.openTable(conf, tableName);
      if(ctable == null) {
        throw new IOException("No table [" + tableName + "]");
      }
      RowFilter rowFilter = new RowFilter();
      
      for(ColumnElement eachElement: selectColumns) {
        List<String> columnNames = eachElement.getColumnNames(ctable);
        for(String eachColumn: columnNames) {
          CellFilter cellFilter = new CellFilter(eachColumn);
          rowFilter.addCellFilter(cellFilter);
        }
      }
      
      if(where != null) {
        where.initFilter(rowFilter);
        Row[] rows = ctable.gets(rowFilter);
        
        status.setRowIterator(new RowArrayIterator(ctable, rows, rowFilter.getColumns()));
        int rowCount = rows == null ? 0 : rows.length;
        
        status.setMessage(rowCount + " selected.");
      } else {
        TableScanner scanner = ScannerFactory.openScanner(ctable, rowFilter);
        status.setRowIterator(new RowScannerIterator(ctable, scanner, rowFilter.getColumns()));
      }
    } catch (IOException e) {
      status.setException(e);
    }
    return status;    
  }

  @Override
  public String getPrefix() {
    return StatementFactory.SELECT;
  }
  
  @Override
  public HelpUsage getHelpUsage() {
    return new HelpUsage(getPrefix(), "Select values from a table.",
        "SELECT <*|colum_name1, ...> \n" +
        " FROM <table_name>" + "\n" +
        " [WHERE rowkey='<row_key>'] \n" +
        " [AND column='<column_key>'] \n" +
        " [AND column.timestamp between 'start(yyyyMMddHHmmss)' and 'end(yyyyMMddHHmmss)']");
  }
}
