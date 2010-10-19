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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.common.exception.GetFailException;
import org.cloudata.core.common.io.CWritable;
import org.cloudata.core.common.io.CWritableUtils;
import org.cloudata.core.common.util.ByteCodeClassLoader;


/**
 * @author jindolk
 *
 */
public class CellValueMatcherInfo implements CWritable {
  
  public static final Log LOG = LogFactory.getLog(CellValueMatcherInfo.class.getName());
  
  private List<String> classNames = new ArrayList<String>();
  private List<byte[]> classBytes = new ArrayList<byte[]>();
  private CellValueMatcher matcher;
  private int version;

  ByteCodeClassLoader classLoader;
 
  public CellValueMatcherInfo() {
    classLoader = new ByteCodeClassLoader(CellValueMatcherInfo.class.getClassLoader()); 
  }
  
  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  public List<String> getClassNames() {
    return classNames;
  }

  public void addClassInfo(Class<?> clazz) throws IOException {
    this.classNames.add(clazz.getName());
    this.classBytes.add(classLoader.getClassBytes(clazz, version));
  }

  public List<byte[]> getClassBytes() {
    return classBytes;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.version = in.readInt();

    int classCount = in.readInt();    
    
    for(int i = 0; i < classCount; i++) {
      String className = CWritableUtils.readString(in);
      int length = in.readInt();
    
      if(length > 0) {
        byte[] classByte = new byte[length];
        in.readFully(classByte);
        
        classNames.add(className);
        classBytes.add(classByte);
      }
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.version);

    int classCount = classNames.size();
    out.writeInt(classCount);
    
    for(int i = 0; i < classCount; i++) {
      CWritableUtils.writeString(out, classNames.get(i));
      byte[] classByte = classBytes.get(i);
      out.writeInt(classByte.length);
      out.write(classByte);
    }
  }

  /**
   * @return
   */
  public CellValueMatcher getCellValueMatcher() throws IOException {
    if(this.matcher != null) {
      return this.matcher;
    }
    int classCount = classNames.size();
    if(classCount == 0) {
      return null;
    }
    
    try {
      for(int i = 0; i < classCount; i++) {
        classLoader.addClassBytes(classNames.get(i), version, classBytes.get(i));
      }
      
      Class<?> matcherClass = classLoader.loadClass(classNames.get(0));
      for(int i = classCount - 1; i > 0; i--) {
        classLoader.loadClass(classNames.get(i));
      }
      this.matcher = (CellValueMatcher)(matcherClass.newInstance());
      return this.matcher;
    } catch (Exception e) {
      LOG.error("CellValueMatcher class load error:" + e.getMessage(), e);
      throw new GetFailException(e);
    } catch (Error e) {
      LOG.error("CellValueMatcher class load error:" + e.getMessage(), e);
      throw new GetFailException(e);
    }
  }
}
