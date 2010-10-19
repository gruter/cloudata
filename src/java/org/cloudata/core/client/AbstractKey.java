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

import java.security.MessageDigest;

import org.cloudata.core.common.io.CText;
import org.cloudata.core.common.io.CWritableUtils;


/**
 * 모든 Key class의 최상위 클래스
 * 
 * @author 김형준
 * 
 */
public abstract class AbstractKey extends CText {
  static MessageDigest md5;

  static {
    try {
      md5 = MessageDigest.getInstance("MD5");
    } catch (Exception e) {

    }
  }

  public AbstractKey() {
    super();
  }

  public AbstractKey(byte[] key) {
    super(key);
  }
  
  public AbstractKey(String key) {
    super(key.getBytes());
  }
  
  public boolean isEmpty() {
    return getLength() <= 0;
  }
  
  public int getByteSize() {
    //map file에 저장될 때에는 길이를 포함해서 저장된다.
    return CWritableUtils.getVIntSize(getLength()) + getLength();
  }
  
  public byte[] nextKeyBytes() {
    byte[] nextKeyBytes = new byte[getLength() + 1];
    System.arraycopy(getBytes(), 0, nextKeyBytes, 0, getLength());
    nextKeyBytes[nextKeyBytes.length - 1] = 0x00;
    return nextKeyBytes;
  }  
}
