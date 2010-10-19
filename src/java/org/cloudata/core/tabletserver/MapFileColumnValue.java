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
package org.cloudata.core.tabletserver;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.cloudata.core.client.Cell;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.io.CWritableUtils;
import org.cloudata.core.tablet.ColumnValue;


public class MapFileColumnValue extends ColumnValue {
  public MapFileColumnValue() {
    super();
  }
  
  public void readFields(DataInput in) throws IOException {
    rowKey.readFields(in);
    cellKey.readFields(in);
    int opCode = in.readInt();
    if(opCode == Constants.DELETEED) {
      deleted = true;
    } else if(opCode == Constants.INSERTED) {
      deleted = false;
    } else {
      throw new IOException("Wrong record operation code(DELETEED or INSERTED): " + opCode);
    }
    timestamp = in.readLong();
    int valueLength = in.readInt();
    value = valueLength < 0 ? null : new byte[valueLength];
    if (value != null) {
      in.readFully(value);
    }   
  }

  public void write(DataOutput out) throws IOException {
    rowKey.write(out);
    
    cellKey.write(out);
    
    //FIXME byte로 변경
    //write operation
    if(deleted) {
      out.writeInt(Constants.DELETEED);
    } else {
      out.writeInt(Constants.INSERTED);
    }

    //write timestame
    out.writeLong(timestamp);
    
    //write value
    int valueLength = (value == null ? -1 : value.length);
    
    out.writeInt(valueLength);
    
    if(valueLength > 0) {
      out.write(value);
    }
  }

  public int size() throws IOException {
    //columnKey + deleted + timestamp + valueSize + value
    return  rowKey.getByteSize() + 
    cellKey.getByteSize() + 
             CWritableUtils.getIntByteSize() + 
             CWritableUtils.getLongByteSize() + 
             CWritableUtils.getIntByteSize() + 
             (value == null ? 0 : value.length);
  }
}
