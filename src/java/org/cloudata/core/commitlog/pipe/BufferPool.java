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
import java.util.HashMap;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class BufferPool {
  static final Log LOG = LogFactory.getLog(BufferPool.class);

  private static final BufferPool singleton = new BufferPool();

  public static BufferPool singleton() {
    return singleton;
  }

  static class PoolEntry implements Comparable<PoolEntry> {
    ByteBuffer buffer;
    long timestamp;

    PoolEntry(long timestamp) {
      this.timestamp = timestamp;
    }

    PoolEntry(ByteBuffer buf) {
      this.buffer = buf;
      this.timestamp = System.currentTimeMillis();
    }

    public int compareTo(PoolEntry o) {
      if (this == o) {
        return 0;
      }
      
      return this.timestamp == o.timestamp ? 1 : (int) (this.timestamp - o.timestamp);
    }
    
    // obj가 this와 같은 객체일 때만 true를 리턴한다.
    public boolean equals(Object obj) {
      if (obj instanceof PoolEntry == false) {
        return false;
      }
      
      return compareTo((PoolEntry) obj) == 0;
    }
  }

  //// MEMBER VARIABLES ////

  HashMap<Integer, TreeSet<PoolEntry>> bufferMap = new HashMap<Integer, TreeSet<PoolEntry>>();
  PoolMonitor poolMonitor;

  private BufferPool() {
    poolMonitor = new PoolMonitor();
    poolMonitor.start();
  }

  public void stop() {
    poolMonitor.interrupt();
    clear();
  }
  
  public void clear() {
    synchronized(bufferMap) {
      bufferMap.clear();
      poolMonitor.clear();
    }
  }
  
  public ByteBuffer[] getBuffer(int size) {
    ByteBuffer retBuffer = null;

    synchronized(bufferMap) {
      TreeSet<PoolEntry> entrySet = bufferMap.get(size);

      if (entrySet != null) {
        PoolEntry entry = entrySet.pollLast();
        if (entry != null) {
          retBuffer = entry.buffer;
        }
      }
    }

    if (retBuffer == null) {
      retBuffer = ByteBuffer.allocateDirect(size);
      poolMonitor.increaseAllocated(size);
    } else {
      poolMonitor.increaseUsed(size);
    }
    
    return new ByteBuffer[] { retBuffer };
  }

  public void returnBuffer(ByteBuffer[] bufferArray) {
    synchronized(bufferMap) {
      for (ByteBuffer buf : bufferArray) {
        buf.clear();
        TreeSet<PoolEntry> entrySet = bufferMap.get(buf.capacity());
        if (entrySet == null) {
          entrySet = new TreeSet<PoolEntry>();
          bufferMap.put(buf.capacity(), entrySet);
        }
        entrySet.add(new PoolEntry(buf));
        poolMonitor.increaseBuffered(buf.capacity());
      }
    }
  }
  
  int clearExpiredEntries(int msec) {
    PoolEntry e = new PoolEntry((System.currentTimeMillis() + 10) - msec);
    int sizeOfDeallocated = 0;

    synchronized(bufferMap) {
      Iterator<TreeSet<PoolEntry>> iter = bufferMap.values().iterator();
      while(iter.hasNext()) {
        TreeSet<PoolEntry> entrySet = iter.next();
        SortedSet<PoolEntry> expiredSet = entrySet.headSet(e);
        
        if (expiredSet.isEmpty() == false) {
          LOG.debug(expiredSet.size() + " pool entries are removed");
          Iterator<PoolEntry> expiredIter = expiredSet.iterator();
          
          while(expiredIter.hasNext()) {
            PoolEntry expiredEntry = expiredIter.next();
            poolMonitor.deallocated(expiredEntry.buffer.capacity());
            sizeOfDeallocated += expiredEntry.buffer.capacity();
          }
          
          expiredSet.clear();
        
          if (entrySet.isEmpty()) {
            LOG.debug("entry set is removed");
            iter.remove();
          }
        }
      }
    }
    
    return sizeOfDeallocated;
  }
  
  public int getTotalAllocatedMemory() {
    return poolMonitor.getTotalAllocatedMemory();
  }
  
  public int getTotalUsedMemory() {
    return poolMonitor.getTotalUsedMemory();
  }

  public int getTotalBufferedMemory() {
    return poolMonitor.getTotalBufferedMemory();
  }


  class PoolMonitor extends Thread {
    int totalAllocatedMemory = 0;
    int totalUsedMemory = 0;
    int totalBufferedMemory = 0;
    
    String lastReport = null;
    long lastGcTime = System.currentTimeMillis();
    
    int totalDeallocatedMemory = 0;
    
    public void run() {
      while (true) {
        try {
          Thread.sleep(5 * 1000);
        } catch (InterruptedException e) {
          break;
        }

        totalDeallocatedMemory += clearExpiredEntries(10000); // 20초가 지난 버퍼는 해제한다.

        String line = null;
        synchronized(this) {
          line = "Allocated : " + totalAllocatedMemory + " Bytes, Used : " 
            + totalUsedMemory + " Bytes, Buffered : " + totalBufferedMemory 
            + " Bytes, Freed : " + totalDeallocatedMemory + " Bytes";
        }
        
        if (lastReport == null || lastReport.equals(line) == false) {
          LOG.info(line);
          lastReport = line;
        }
        
        if (totalDeallocatedMemory > (totalAllocatedMemory / 4)) {
          long beforeGc = System.currentTimeMillis();
          System.gc();
          LOG.info("TIME [Gabage Collection] takes " + (System.currentTimeMillis() - beforeGc) + "ms");
          totalDeallocatedMemory = 0;
        }
      }
      
      LOG.info("BufferPool monitoring thread ends");
    }

    public int getTotalBufferedMemory() {
      return totalBufferedMemory;
    }

    public int getTotalUsedMemory() {
      return totalUsedMemory;
    }

    public int getTotalAllocatedMemory() {
      return totalAllocatedMemory;
    }

    public synchronized void clear() {
      totalAllocatedMemory = 0;
      totalUsedMemory = 0;
      totalBufferedMemory = 0;
    }

    public synchronized void increaseAllocated(int size) {
      totalAllocatedMemory += size;
      totalUsedMemory += size;
    }
    
    public synchronized void increaseUsed(int size) {
      totalBufferedMemory -= size;
      totalUsedMemory += size;
    }
    
    public synchronized void increaseBuffered(int size) {
      totalBufferedMemory += size;
      totalUsedMemory -= size;
    }
    
    public synchronized void deallocated(int size) {
      totalBufferedMemory -= size;
      totalAllocatedMemory -= size;
    }
  }
}
