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
package org.cloudata.core.tablet;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.cloudata.core.client.Cell;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.io.CWritable;
import org.cloudata.core.common.io.CWritableUtils;


/**
 * 특정 row의 특정 column에 저장되어 있는 모든 값을 가진다(ValueObject).<br>
 * @author 김형준
 *
 */
public class RowColumnValues implements CWritable, Comparable<RowColumnValues>, org.apache.hadoop.io.Writable {
	private Row.Key rowKey;
	private String columnName;
  private ColumnValue[] columnValues;
	
	public RowColumnValues() {
		rowKey = new Row.Key();
	}

	public RowColumnValues(String columnName, Row.Key rowKey, ColumnValue[] columnValues) {
		this.rowKey = rowKey;
		this.columnName = columnName;
		this.columnValues = columnValues;
	}
	
	public void readFields(DataInput in) throws IOException {
		rowKey.readFields(in);
		columnName = CWritableUtils.readString(in);
		
		int recordCount = in.readInt();
		
		columnValues = new ColumnValue[recordCount];
		for(int i = 0; i < recordCount; i++) {
      int flag = in.readInt();
      if(flag == 0) {
        columnValues[i] = null;
      } else {
  			columnValues[i] = new ColumnValue();
  			columnValues[i].readFields(in);
      }
		}
	}

	public void write(DataOutput out) throws IOException {
		rowKey.write(out);
		CWritableUtils.writeString(out, columnName);

		int recordCount = (columnValues == null ? 0 : columnValues.length);
		out.writeInt(recordCount);
		
		for(int i = 0; i < recordCount; i++) {
      if(columnValues[i] == null) {
        out.writeInt(0);
      } else {
        out.writeInt(1);
        columnValues[i].write(out);
      }
		}
	}

	public String getColumnName() {
		return columnName;
	}

	public Row.Key getRowKey() {
		return rowKey;
	}

	public ColumnValue[] getColumnRecords() {
		return columnValues;
	}

	public void setColumnRecords(ColumnValue[] columnValues) {
		this.columnValues = columnValues;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public void setRowKey(Row.Key rowKey) {
		this.rowKey = rowKey;
	}


	public int compareTo(RowColumnValues rowColumnValues) {
    String str1 = rowKey.toString() + columnName;
    String str2 = rowColumnValues.rowKey.toString() + rowColumnValues.columnName;
    return str1.compareTo(str2);
	}

  public boolean equals(Object obj) {
    if( !(obj instanceof RowColumnValues) ) {
      return false;
    }
    
    return compareTo((RowColumnValues)obj) == 0;
  } 
  
	public void print() {
		if(rowKey == null || rowKey.getBytes() == null) {
			System.out.println("empty result");
			return;
		}
		
		System.out.println("Row.Key:" + rowKey + ", ColumnName:" + columnName + ", count:" + columnValues.length);
		if(columnValues == null || columnValues.length == 0) {
			System.out.println("\tempty");
			return;
		}
		for(ColumnValue columnValue: columnValues) {
			System.out.println("\t" + columnValue);
		}
	}
  
  public String toString() {
    return "Row.Key:" + rowKey + ", ColumnName:" + columnName + ", count:" + columnValues.length;
  }
  
  public static Row makeRow(RowColumnValues[] values) {
    if(values == null || values.length == 0) {
      return null;
    }
    
    Row row = new Row(values[0].getRowKey());
    for(RowColumnValues eachValues: values) {
      //eachValues가 하나의 column의 모든 데이터를 가지고 있다.
      ColumnValue[] columnValues = eachValues.getColumnRecords();
      
      if(columnValues == null) {
        continue;
      }
      
      Cell.Key previousKey = null;
      
      List<Cell.Value> cellValues = new ArrayList<Cell.Value>();
      for(ColumnValue eachColumnValue: columnValues) {
        //System.out.println(">>>" + eachColumnValue);
        Cell.Key key = eachColumnValue.getCellKey();
        if(previousKey != null && !previousKey.equals(key)) {
          Cell cell = new Cell(previousKey);
          cell.setValues(cellValues);
          row.addCell(eachValues.getColumnName(), cell);
          cellValues.clear();
        }
        
        cellValues.add(eachColumnValue.copyToCellValue());
        previousKey = key;      
      }
      if(cellValues.size() > 0) {
        Cell cell = new Cell(previousKey);
        cell.setValues(cellValues);
        row.addCell(eachValues.getColumnName(), cell);
        cellValues.clear();
      }
    }
    return row;
  } 
}
