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
package org.cloudata.tools.cloudatafs;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Date;

import org.cloudata.core.common.io.CWritable;
import org.cloudata.core.common.io.CWritableUtils;


/**
 * @author jindolk
 *
 */
public class FileNode implements CWritable {
  private static DecimalFormat df = new DecimalFormat("###,###,###,###,###,###");
  
  public static int DIR = 0;
  public static int FILE = 1;
  
  private int version;  
  private String path;
  private int type;
  private long date;
  private long length;
  private String user;
  private String group;
  private String mode;
  
  public FileNode() throws IOException {
    date = System.currentTimeMillis();
  }
  
  public FileNode(byte[] data) throws IOException {
    readFields(new DataInputStream(new ByteArrayInputStream(data)));
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    version = in.readInt();  
    path = CWritableUtils.readString(in);
    type = in.readInt();
    date = in.readLong();
    length = in.readLong();
    user = CWritableUtils.readString(in);
    group = CWritableUtils.readString(in);
    mode = CWritableUtils.readString(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(version);  
    CWritableUtils.writeString(out, path);
    out.writeInt(type);
    out.writeLong(date);
    out.writeLong(length);
    CWritableUtils.writeString(out, user);
    CWritableUtils.writeString(out, group);
    CWritableUtils.writeString(out, mode);
  }
  
  public int getVersion() {
    return version;
  }
  public void setVersion(int version) {
    this.version = version;
  }
  public int getType() {
    return type;
  }
  public void setType(int type) {
    this.type = type;
  }
  public long getDate() {
    return date;
  }
  public void setDate(long date) {
    this.date = date;
  }
  public long getLength() {
    return length;
  }
  public void setLength(long length) {
    this.length = length;
  }
  public String getUser() {
    return user;
  }
  public void setUser(String user) {
    this.user = user;
  }
  public String getGroup() {
    return group;
  }
  public void setGroup(String group) {
    this.group = group;
  }
  public String getMode() {
    return mode;
  }
  public void setMode(String mode) {
    this.mode = mode;
  }
  public String getPath() {
    return path;
  }
  
  public void setPath(String path) {
    this.path = path;
  }

  public String getPathName() {
    return CloudataFS.getPathName(path);
  }
  
  public boolean isDir() {
    return type == DIR;
  }
  
  public byte[] getBytes() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    
    write(new DataOutputStream(out));
    
    return out.toByteArray();
  }
  
  public String toJsonString(String id) {
    String image = WebUtil.getImage(this);
    
    String result = 
      "\"id\":\"" + id +"\"," +
      "\"cell\":[" ;
    
    result += "\"" + getPathName() + "\",";  
    
    if(image != null) {
      result += "\"<img src='" + image + "\'>" + getPathName() + "\"," ;
    } else {
      result += "\"" + getPathName() + "\"," ;
    }
    result += "\"" + (type == DIR ? "": df.format(length)) + "\"," +
      "\"" + (type == DIR ? "dir": "file") + "\"," +
      "\"" + new Date(date) + "\"," +
      "\"" + "" + "\"]" ;   //user

    return result;
  }

  public String getExt() {
    int index = path.lastIndexOf(".");
    if(index <= 0) {
      return null;
    } else {
      return path.substring(index + 1);
    }
  }
}
