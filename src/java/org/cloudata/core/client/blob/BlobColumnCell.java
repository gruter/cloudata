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
package org.cloudata.core.client.blob;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.cloudata.core.common.io.CWritable;
import org.cloudata.core.common.io.CWritableUtils;


/**
 * @author jindolk
 *
 */
public class BlobColumnCell implements CWritable {
  private String filePath;
  private long offset;
  private long length;
  
  public BlobColumnCell() {
  }
  
  public BlobColumnCell(String filePath, long offset, long length) {
    this.filePath = filePath;
    this.offset = offset;
    this.length = length;
  }
  
  public BlobColumnCell(byte[] data) throws IOException {
    readFields(new DataInputStream(new ByteArrayInputStream(data)));
  }
  
  public String toString() {
    return filePath + "," + offset + "," + length;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    filePath = CWritableUtils.readString(in);
    offset = in.readLong();
    length = in.readLong();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    CWritableUtils.writeString(out, filePath);
    out.writeLong(offset);
    out.writeLong(length);
  }
  
  public byte[] getBytes() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    
    write(new DataOutputStream(out));
    
    return out.toByteArray();
  }

  public String getFilePath() {
    return filePath;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public long getLength() {
    return length;
  }

  public void setLength(long length) {
    this.length = length;
  }
}
