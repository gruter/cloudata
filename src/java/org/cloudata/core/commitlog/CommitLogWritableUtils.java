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
package org.cloudata.core.commitlog;

import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CommitLogWritableUtils {
  static final Log LOG = LogFactory.getLog(CommitLogWritableUtils.class);

  public static String readString(ByteBuffer buf) {
    int stringLength = buf.getInt();
    byte[] stringBuf = new byte[stringLength];
    buf.get(stringBuf);
    return new String(stringBuf);
  }

  public static String[] readStringArray(ByteBuffer buf) {
    int length = buf.getInt();
    if (length > 1000000) {
      LOG.warn("the length in unmarshalling string array is too big : " + length);
    }
    String[] ret = new String[length];
    for (int i = 0; i < length; i++) {
      ret[i] = readString(buf);
    }

    return ret;
  }

  public static void writeString(ByteBuffer buf, String s) {
    byte[] sBytes = s.getBytes();

    buf.putInt(sBytes.length);
    buf.put(sBytes);
  }

  public static void writeStringArray(ByteBuffer buf, String[] array) {
    buf.putInt(array.length);

    for (int i = 0; i < array.length; i++) {
      writeString(buf, array[i]);
    }
  }

  // ////////// for debugging ////////////////
  public static void dump(byte[] array) {
    dumpTo(array, array.length);
  }

  public static void dumpTo(byte[] array, int length) {
    dumpTo(array, 0, length);
  }

  public static synchronized void dumpTo(byte[] array, int start, int length) {
    dumpTo(array, 0, length, null);
  }

  public static synchronized void dumpTo(byte[] array, int start, int length, String info) {
    System.out.println("=== START DUMP ===");

    if (info != null) {
      System.out.println(info);
    }

    System.out.println("byte length : " + length);

    for (int i = start; i < start + length; i++) {
      if (i % 16 == 0) {
        System.out.println("");
      } else if (i % 8 == 0) {
        System.out.print(" ");
      }
      System.out.format("[0x%2x]", array[i]);
    }
    System.out.println("");
    System.out.println("=== END DUMP ===");
  }
}
