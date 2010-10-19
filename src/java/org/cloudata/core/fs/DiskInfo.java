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
package org.cloudata.core.fs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.cloudata.core.common.io.CWritable;
import org.cloudata.core.common.io.CWritableUtils;


/**
 * @author jindolk
 *
 */
public class DiskInfo implements CWritable {
  protected String filesystem;
  protected long capacity;
  protected long used;
  protected long available;
  protected int percentUsed;
  protected String mount;
  
  public String getFilesystem() {
    return filesystem;
  }
  public void setFilesystem(String filesystem) {
    this.filesystem = filesystem;
  }
  public long getCapacity() {
    return capacity;
  }
  public void setCapacity(long capacity) {
    this.capacity = capacity;
  }
  public long getUsed() {
    return used;
  }
  public void setUsed(long used) {
    this.used = used;
  }
  public long getAvailable() {
    return available;
  }
  public void setAvailable(long available) {
    this.available = available;
  }
  public int getPercentUsed() {
    return percentUsed;
  }
  public void setPercentUsed(int percentUsed) {
    this.percentUsed = percentUsed;
  }
  public String getMount() {
    return mount;
  }
  public void setMount(String mount) {
    this.mount = mount;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    filesystem = CWritableUtils.readString(in);
    capacity = in.readLong();
    used = in.readLong();
    available = in.readLong();
    percentUsed = in.readInt();
    mount = CWritableUtils.readString(in);
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    CWritableUtils.writeString(out, filesystem);
    out.writeLong(capacity);
    out.writeLong(used);
    out.writeLong(available);
    out.writeInt(percentUsed);
    CWritableUtils.writeString(out, mount);    
  }
}
