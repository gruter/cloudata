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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.io.CWritable;
import org.cloudata.core.tablet.ColumnValue;


/**
 * Cloudata stores multiple values in a point of Row-Column.<BR>
 * Cell is Value Object which is stored that point. Cell is identified by Cell.Key in a Row.<BR>
 * A Cell has also multiple Cell.Value objects. Cell.Value is identified by timestamp.
 */
public class Cell implements CWritable, Writable, Comparable<Cell> {
  Key key;
  List<Value> values;
  
  public Cell() {
  }
  
  public Cell(Cell.Key key) {
    //this(key, null);
    this.key = key;
  }
  
  public Cell(Cell.Key key, byte[] bytes) {
    this.key = key;
    addValue(new Value(bytes));
  }
  
  public Cell(Key key, byte[] bytes, long timestamp) {
    this.key = key;
    addValue(new Value(bytes, timestamp));
  }

  /**
   * Add Cell.Value
   * @param value
   */
  public void addValue(Value value) {
    if(values == null) {
      values = new ArrayList<Value>();
    }
    
    values.add(value);
  }
  
  public Key getKey() {
    return key;
  }
  
  public void setKey(Key key) {
    this.key = key;
  }
  
  /**
   * Return latest Cell.Value's bytes array
   * @return
   */
  public byte[] getBytes() {
    if(values == null || values.size() == 0) {
      return null;
    }
    return values.get(0).getBytes();
  }
  
  /**
   * Return (latest Cell.Value's bytes array).toString()
   * @return
   */
  public String getValueAsString() {
    if(values == null || values.size() == 0) {
      return null;
    }
    return values.get(0).getValueAsString();
  }
  
  /**
   * Return latest Cell.Value
   * @return
   */
  public Value getValue() {
    if(values == null || values.size() == 0) {
      return null;
    }
    return values.get(0);
  }
  
  /**
   * Return Cell.Value <BR> 
   * Cell.Value is sorted by timestamp descending 
   * @param index
   * @return
   */
  public Value getValue(int index) {
    if(values == null || values.size() == 0 || index >= values.size()) {
      return null;
    }
    return values.get(index);
  }
  
  /**
   * Return number of Cell.Value<BR>
   * This value is not number of version in a Table, but number of Value which is matched with search condition. 
   * @return
   */
  public int getValueSize() {
    if(values == null || values.size() == 0) {
      return 0;
    }
    
    return values.size();
  }
  
  public List<Value> getValues() {
    return values;
  }

  public Value getLastValue() {
    int size = getValueSize();
    if(size == 0) {
      return null;
    }
    return values.get(size - 1);
  }

  public void setValues(List<Value> values) {
    this.values = new ArrayList<Value>();
    this.values.addAll(values);
  }
  
  public String getPrintValue(String encoding) {
    try {
      String keyStr = null;
      if(key != null || key.getBytes() != null) {
        keyStr = new String(key.getBytes(), 0, key.getLength(), encoding);
      }
      
      String valueStr = null;
      Value value = getValue();
      if(value != null && value.bytes != null) {
        valueStr = new String(value.bytes, 0, value.bytes.length, encoding);
      }
      
      if(value.numOfValues > 1) {
        return keyStr + ":" + valueStr + "(" + value.numOfValues + ")";
      } else {
        return keyStr + ":" + valueStr;
      }
    } catch (UnsupportedEncodingException e) {
      return "Error:" + e.getMessage();
    } catch (Exception e) {
      e.printStackTrace();
      return "";
    }
  }
  
  public String getPrintValue() {
    return getPrintValue("utf-8");
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    key = new Key();
    values = new ArrayList<Value>();
    
    key.readFields(in);
    
    int length = in.readInt();
    for(int i = 0; i < length; i++) {
      Value value = new Value();
      value.readFields(in);
      values.add(value);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    if(key != null) {
      key.write(out);
    }

    int length = values == null ? 0 : values.size();
    out.writeInt(length);

    if(values != null) {
      for(Value eachValue: values) {
        eachValue.write(out);
      }
    }
  }

  @Override
  public int compareTo(Cell o) {
    if(key == null || key.getBytes() == null)   return -1;
    if(o.key == null || o.key.getBytes() == null)   return 1;
    
    return key.compareTo(o.key);
  }
  
  public boolean equals(Object obj) {
    if( !(obj instanceof Cell) ) {
      return false;
    }
    
    return compareTo((Cell)obj) == 0;
  }   
  
  /**
   * Cell.Key class
   * @author jindolk
   *
   */
  public static class Key extends AbstractKey {
    public static final Key EMPTY_KEY = new Key("");
    public static final Key MAX_KEY = new Key(Constants.MAX_VALUE);
    
    public Key() {
      super();
    }

    public Key(byte[] key) {
      super(key);
    }
    
    public Key(String key) {
      super(key);
    }
  }
  
  /**
   * Cell value
   *
   */
  public static class Value implements CWritable, Writable, Comparable<Value> {
    protected byte[] bytes;
    
    protected long timestamp;
    
    protected boolean deleted;
    
    private int numOfValues;

    public Value () {
      
    }
    
    public Value(byte[] bytes) {
      this.bytes = bytes;
    }

    public Value(byte[] bytes, long timestamp) {
      this.bytes = bytes;
      this.timestamp = timestamp;
    }
    
    public Value(byte[] bytes, long timestamp, boolean deleted) {
      this.bytes = bytes;
      this.timestamp = timestamp;
      this.deleted = deleted;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      int opCode = in.readInt();
      if(opCode == Constants.DELETEED) {
        deleted = true;
      } else if(opCode == Constants.INSERTED) {
        deleted = false;
      } else {
        throw new IOException("Wrong record operation code(DELETEED or INSERTED): " + opCode);
      }
      timestamp = in.readLong();
      numOfValues = in.readInt();
      int valueLength = in.readInt();
      bytes = null;
      if(valueLength >= 0) {
        bytes = new byte[valueLength];
        in.readFully(bytes);
      }  
    }

    @Override
    public void write(DataOutput out) throws IOException {
      //write operation
      if(deleted) {
        out.writeInt(Constants.DELETEED);
      } else {
        out.writeInt(Constants.INSERTED);
      }

      //write timestame
      out.writeLong(timestamp);
      
      //write numOfValues
      out.writeInt(numOfValues);

      //write value
      int valueLength = (bytes == null ? -1 : bytes.length);
      
      out.writeInt(valueLength);
      
      if(valueLength > 0) {
        out.write(bytes);
      }      
    }

    @Override
    public int compareTo(Value o) {
      if(timestamp == o.timestamp) {
        if(deleted == o.deleted) {
          return 0;
        } else {
          if(deleted) {
            return -1;
          } else {
            return 1;
          }
        }
      } else {
        return 1;
      }
    }
    
    public boolean equals(Value obj) {
      if( !(obj instanceof Value) ) {
        return false;
      }
      
      return compareTo((Value)obj) == 0;
    }  
    
    public long getTimestamp() {
      return timestamp;
    }

    public void setTimestamp(long timestamp) {
      this.timestamp = timestamp;
    }

    public int getNumOfValues() {
      return numOfValues;
    }

    public void setNumOfValues(int numOfValues) {
      this.numOfValues = numOfValues;
    }

    public long getValueAsLong() {
      return Long.parseLong(new String(bytes));
    }
    
    public String getValueAsString() {
      if(bytes == null) {
        return null;
      }
      return new String(bytes);
    }
    
    public int getValueAsInt() {
      return Integer.parseInt(new String(bytes));
    }

    public byte[] getBytes() {
      return bytes;
    }
    
    public boolean isDeleted() {
      return deleted;
    }

    public void setBytes(byte[] bytes) {
      this.bytes = bytes;
    }

    public void setDeleted(boolean deleted) {
      this.deleted = deleted;
    }

    public void copyToColumnValue(ColumnValue columnValue) {
      columnValue.setDeleted(deleted);
      columnValue.setNumOfValues(numOfValues);
      columnValue.setTimestamp(timestamp);
      columnValue.setValue(bytes);
    }
    
    public int hashCode() {
      return toString().hashCode();
    }
    
    public String toString() {
      return (new String(bytes)) + "," + timestamp + "," + deleted + "," + numOfValues;
    }
  }
}
