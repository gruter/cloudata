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
package org.cloudata.util.matrix;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.WritableComparable;
import org.cloudata.core.common.io.CWritableUtils;


public class MatrixItem implements WritableComparable {
  String row;

  String column;

  double value;

  public MatrixItem() {
    
  }
  
  public String toString() {
    return "[" + row + "," + column + "=" + value + "]";
  }

  public void readFields(DataInput in) throws IOException {
    row = CWritableUtils.readString(in);
    column = CWritableUtils.readString(in);
    value = in.readDouble();
  }

  public void write(DataOutput out) throws IOException {
    CWritableUtils.writeString(out, row);
    CWritableUtils.writeString(out, column);
    out.writeDouble(value);
  }

  public boolean equals(Object o) {
    return compareTo(o) == 0;
  }
  
  public int hashCode() {
    return (row + column).toString().hashCode();
  }
  
  public int compareTo(Object o) {
    if(o == null) {
      return 1;
    }
    MatrixItem target = (MatrixItem)o;
    
    if(row.equals(target.row)) {
      return column.compareTo(target.column);
    } else {
      return row.compareTo(target.row);
    }
  }

  public String getColumn() {
    return column;
  }

  public void setColumn(String column) {
    this.column = column;
  }

  public String getRow() {
    return row;
  }

  public void setRow(String row) {
    this.row = row;
  }

  public double getValue() {
    return value;
  }

  public void setValue(double value) {
    this.value = value;
  }
  
//  public static void main(String[] args) {
//    Map<MatrixItem, Double> map = new HashMap<MatrixItem, Double>();
//    MatrixItem a = new MatrixItem();
//    a.row = "00000000815";
//    a.column = "00000000033";
//    a.value = 100;
//    map.put(a, a.value);
//    
//    MatrixItem b = new MatrixItem();
//    b.row = "00000000815";
//    b.column=  "00000000033";
//    b.value = 100;
//    
//    System.out.println(a.equals(b) + ">" + map.containsKey(b));
//  }
}
