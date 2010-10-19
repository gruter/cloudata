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
package org.cloudata.core.client.cql.element;

import java.io.IOException;

import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CellFilter;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.RowFilter;
import org.cloudata.core.common.conf.CloudataConf;


/**
 * @author jindolk
 *
 */
public class BoolExpression implements WhereExpressionElement {
  Operator op;
  SingleColumnElement column;
  StringLiteral value;

  public void setColumn(SingleColumnElement column) {
    this.column = column;
  }
  
  public void setValue(StringLiteral value) {
    this.value = value;
  }
  
  public void setOperator(Operator op) {
    this.op = op;
  }
  
  public void setOperator(String op) {
    if("=".equals(op)) {
      this.op = new Operator(Operator.OPERATOR.EQ);
    } else if(">".equals(op)) {
      this.op = new Operator(Operator.OPERATOR.GT);
    } else if(">=".equals(op)) {
      this.op = new Operator(Operator.OPERATOR.GT_EQ);
    } else if("<".equals(op)) {
      this.op = new Operator(Operator.OPERATOR.LT);
    } else if("<=".equals(op)) {
      this.op = new Operator(Operator.OPERATOR.LT_EQ);
    }
  }

  @Override
  public String getQuery(CloudataConf conf) {
    return column.getQuery(conf) + " " + op.getQuery(conf) + " " + value.getQuery(conf);
  }

  public Operator getOp() {
    return op;
  }

  public void setOp(Operator op) {
    this.op = op;
  }

  @Override
  public void initFilter(RowFilter rowFilter) throws IOException {
    if(column.isRowKey()) {
      if(op.getOp() != Operator.OPERATOR.EQ) {
        throw new IOException("Only support equal operation for rowkey");
      }
      Row.Key rowKey = new Row.Key(value.getValue());
      rowFilter.setValue(rowKey, rowKey, RowFilter.OP_EQ);
    } else {
      CellFilter cellFilter = rowFilter.getCellFilterInfo(column.getColumnName());
      if(cellFilter == null) {
        cellFilter = new CellFilter(column.getColumnName());
        rowFilter.addCellFilter(cellFilter);
      }
      
      cellFilter.setCellKey(new Cell.Key(value.getValue()));
    }    
  }
}
