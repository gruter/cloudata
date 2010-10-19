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
package org.cloudata.core.commitlog.pipe;

import java.nio.ByteBuffer;

import org.cloudata.core.commitlog.pipe.BufferPool;


import junit.framework.TestCase;

public class TestBufferPool extends TestCase {
  BufferPool pool; 
    
  protected void setUp() throws Exception {
    pool = BufferPool.singleton();
    pool.clear();
  }

  protected void tearDown() throws Exception {
    pool.stop();
  }
  
  public void testGetBuffer1() {

    
    // get 500 byte buffer
    ByteBuffer[] bufferList = pool.getBuffer(500);

    assertEquals(1, bufferList.length);
    assertEquals(500, getTotalCapacity(bufferList));
    assertEquals(500, pool.poolMonitor.totalAllocatedMemory);
    assertEquals(500, pool.poolMonitor.totalUsedMemory);
    assertEquals(0, pool.poolMonitor.totalBufferedMemory);
    
    pool.returnBuffer(bufferList);
    
    assertEquals(500, pool.getTotalAllocatedMemory());
    assertEquals(0, pool.getTotalUsedMemory());
    assertEquals(500, pool.getTotalBufferedMemory());

    // get 1000 byte buffer
    bufferList = pool.getBuffer(1000);
    
    assertEquals(1500, pool.poolMonitor.totalAllocatedMemory);
    assertEquals(500, pool.poolMonitor.totalBufferedMemory);
    assertEquals(1000, pool.poolMonitor.totalUsedMemory);

    assertEquals(1, bufferList.length);
    assertEquals(1000, getTotalCapacity(bufferList));
    
    pool.returnBuffer(bufferList);

    assertEquals(1500, pool.getTotalAllocatedMemory());
    assertEquals(1500, pool.getTotalBufferedMemory());
    assertEquals(0, pool.getTotalUsedMemory());
    
    //해당 크기의 버퍼가 없을 경우 작은 버퍼들을 모아서 구성한다.
    // get 1500 byte buffer
//    bufferList = pool.getBuffer(1500);
//
//    assertEquals(2, bufferList.length);
//    assertEquals(1500, getTotalCapacity(bufferList));
//    
//    pool.returnBuffer(bufferList);
  }
  
  public void testGetBuffer2() {
    ByteBuffer[] bufferList = pool.getBuffer(1000);
    ByteBuffer[] bufferList2 = pool.getBuffer(1000);
    
    pool.returnBuffer(bufferList);
    try {
      Thread.sleep(100);
    } catch(InterruptedException e) {
    }
    pool.returnBuffer(bufferList2);
    
    assertTrue(bufferList2[0] == pool.getBuffer(1000)[0]);
    assertTrue(bufferList[0] == pool.getBuffer(1000)[0]);
  }
  
  public void testReleaseBuffer1() {
    ByteBuffer[] buffer = pool.getBuffer(1500);
    pool.returnBuffer(buffer);
    
    try {
      Thread.sleep(1000);
    } catch(Exception e) {
    }
    
    buffer = pool.getBuffer(2000);
    pool.returnBuffer(buffer);
    
    assertEquals(2, pool.bufferMap.size());
    
    assertEquals(3500, pool.poolMonitor.totalAllocatedMemory);
    assertEquals(3500, pool.poolMonitor.totalBufferedMemory);
    assertEquals(0, pool.poolMonitor.totalUsedMemory);
    
    pool.clearExpiredEntries(1000); // return 된지 1초가 지난 버퍼는 해제한다.
    assertEquals(1, pool.bufferMap.size());
    
    assertEquals(2000, pool.poolMonitor.totalAllocatedMemory);
    assertEquals(2000, pool.poolMonitor.totalBufferedMemory);
    assertEquals(0, pool.poolMonitor.totalUsedMemory);
  }
  
  public void testReleaseBuffer2() {
    ByteBuffer[] buffer = pool.getBuffer(2000);
    ByteBuffer[] buffer2 = pool.getBuffer(2000);
    pool.returnBuffer(buffer);

    try {
      Thread.sleep(1000);
    } catch(Exception e) {
    }
    
    pool.returnBuffer(buffer2);
    
    assertEquals(1, pool.bufferMap.size());
    assertEquals(2, pool.bufferMap.get(2000).size());
    
    assertEquals(4000, pool.poolMonitor.totalAllocatedMemory);
    assertEquals(4000, pool.poolMonitor.totalBufferedMemory);
    assertEquals(0, pool.poolMonitor.totalUsedMemory);
    
    pool.clearExpiredEntries(1000); // return 된지 1초가 지난 버퍼는 해제한다.
    assertEquals(1, pool.bufferMap.get(2000).size());
    assertTrue(buffer2[0] == pool.bufferMap.get(2000).first().buffer);
    
    assertEquals(2000, pool.poolMonitor.totalAllocatedMemory);
    assertEquals(2000, pool.poolMonitor.totalBufferedMemory);
    assertEquals(0, pool.poolMonitor.totalUsedMemory);
    
    try {
      Thread.sleep(100);
    } catch(Exception e) {
    }
    
    pool.clearExpiredEntries(100); // return 된지 0.1초가 지난 버퍼는 해제한다.

    assertEquals(0, pool.poolMonitor.totalAllocatedMemory);
    assertEquals(0, pool.poolMonitor.totalBufferedMemory);
    assertEquals(0, pool.poolMonitor.totalUsedMemory);
  }
  
  public void testReleaseBuffer3() {
    ByteBuffer[] buffer = pool.getBuffer(2000);
    ByteBuffer[] buffer2 = pool.getBuffer(2000);

    pool.returnBuffer(buffer2);
    pool.returnBuffer(buffer);
    
    assertEquals(4000, pool.poolMonitor.totalAllocatedMemory);
    assertEquals(4000, pool.poolMonitor.totalBufferedMemory);
    assertEquals(0, pool.poolMonitor.totalUsedMemory);
    assertEquals(2, pool.bufferMap.get(2000).size());
    
    try {
      Thread.sleep(100);
    } catch(Exception e) {
    }
    
    pool.clearExpiredEntries(100);
    
    assertEquals(0, pool.poolMonitor.totalAllocatedMemory);
    assertEquals(0, pool.poolMonitor.totalBufferedMemory);
    assertEquals(0, pool.poolMonitor.totalUsedMemory);
    assertNull(pool.bufferMap.get(2000));
  }

  private int getTotalCapacity(ByteBuffer[] bufferList) {
    int totalCapacity = 0;
    for(ByteBuffer buf : bufferList) {
      totalCapacity += buf.capacity();
    }
    return totalCapacity;
  }
}
