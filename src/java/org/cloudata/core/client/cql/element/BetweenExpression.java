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
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.cloudata.core.client.CellFilter;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.RowFilter;
import org.cloudata.core.common.conf.CloudataConf;


/**
 * @author jindolk
 *
 */
public class BetweenExpression implements WhereExpressionElement {
  SingleColumnElement column;
  StringLiteral fromValue;
  StringLiteral toValue;
  
  static final SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
  
  public void setColumn(SingleColumnElement column) {
    this.column = column;
  }
  
  public void setFromValue(StringLiteral fromValue) {
    this.fromValue = fromValue;
  }

  public void setToValue(StringLiteral toValue) {
    this.toValue = toValue;
  }

  @Override
  public String getQuery(CloudataConf conf) {
    return column.getQuery(conf) + " BETWEEN " + 
                  fromValue.getQuery(conf) + " AND " + toValue.getQuery(conf);
  }
  
  @Override
  public void initFilter(RowFilter rowFilter) throws IOException {
    if(column.isRowKey()) {
      rowFilter.setValue(new Row.Key(fromValue.getValue()), 
          new Row.Key(toValue.getValue()), 
          RowFilter.OP_BETWEEN);
    } else {
      if(!column.isTimestamp()) {
        throw new IOException("Not support column's between expression");
      }
      CellFilter cellFilter = rowFilter.getCellFilterInfo(column.getColumnName());
      if(cellFilter == null) {
        cellFilter = new CellFilter(column.getColumnName());
        rowFilter.addCellFilter(cellFilter);
      }
      String startDate = fromValue.getValue();
      if(startDate.length() == 8) {
        startDate += "000000";
      } else if(startDate.length() == 14) {
      } else {
        throw new IOException("timestamp format must be yyyyMMdd or yyyyMMddHHmmss");
      }

      String endDate = toValue.getValue();
      if(endDate.length() == 8) {
        endDate += "235959";
      } else if(endDate.length() == 14) {
      } else {
        throw new IOException("timestamp format must be yyyyMMdd or yyyyMMddHHmmss");
      }

      try {
        cellFilter.setTimestamp(df.parse(startDate).getTime(), df.parse(endDate).getTime());
      } catch (ParseException e) {
        throw new IOException(e.getMessage());
      }
    }
  }
}
