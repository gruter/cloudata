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
package org.cloudata.core.common.util;

import java.io.IOException;

public class SizeOf {
  public static void main(String[] args) throws Exception {
    // Warm up all classes/methods we will use
    runGC();
    usedMemory();
    // Array to keep strong references to allocated objects
    final int count = 100000;
    Object[] objects = new Object[count];

    long heap1 = 0;
    // Allocate count+1 objects, discard the first one
    for (int i = -1; i < count; ++i) {
      Object object = null;

      // Instantiate your data here and assign it to object

//      object = new Object();
      // object = new Integer (i);
      // object = new Long (i);
      // object = new String ();
       object = new byte [1];

      if (i >= 0)
        objects[i] = object;
      else {
        object = null; // Discard the warm up object
        runGC();
        heap1 = usedMemory(); // Take a before heap snapshot
      }
    }
    runGC();
    long heap2 = usedMemory(); // Take an after heap snapshot:

    final int size = Math.round(((float) (heap2 - heap1)) / count);
    System.out.println("'before' heap: " + heap1 + ", 'after' heap: " + heap2);
    System.out.println("heap delta: " + (heap2 - heap1) + ", {"
        + objects[0].getClass() + "} size = " + size + " bytes");
    for (int i = 0; i < count; ++i)
      objects[i] = null;
    objects = null;
  }

  public static void runGC() throws IOException {
    // It helps to call Runtime.gc()
    // using several method calls:
    try {
      for (int r = 0; r < 4; ++r)
        _runGC();
    } catch (Exception e) {
      IOException exception = new IOException("Error:runGC");
      exception.initCause(e);
      throw exception;
    }
  }

  private static void _runGC() throws Exception {
    long usedMem1 = usedMemory(), usedMem2 = Long.MAX_VALUE;
    for (int i = 0; (usedMem1 < usedMem2) && (i < 500); ++i) {
      s_runtime.runFinalization();
      s_runtime.gc();
      Thread.currentThread().yield();

      usedMem2 = usedMem1;
      usedMem1 = usedMemory();
    }
  }

  private static long usedMemory() {
    return s_runtime.totalMemory() - s_runtime.freeMemory();
  }

  public static long freeMemory() {
    return s_runtime.freeMemory();
  }
  
  public static long gcAndfreeMemory() throws IOException {
    try {
      runGC();
    } catch (Exception e) {
      IOException exception = new IOException("Error:gcAndfreeMemory");
      exception.initCause(e);
      throw exception;
    }
    return s_runtime.freeMemory();
  }
  
  public static long totalMemory() {
    return s_runtime.totalMemory();
  }

  public static long maxMemory() {
    return s_runtime.maxMemory();
  }
  
  public static long getRealFreeMemory() {
    return s_runtime.maxMemory() - usedMemory();
  }
  
  private static final Runtime s_runtime = Runtime.getRuntime();
}
