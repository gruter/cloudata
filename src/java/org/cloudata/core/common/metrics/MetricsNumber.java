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
package org.cloudata.core.common.metrics;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jindolk
 *
 */
public class MetricsNumber {
  private AtomicLong count = new AtomicLong(0);
  private AtomicLong lastValue = new AtomicLong(0);
  private AtomicLong currentValue = new AtomicLong(0);
  private AtomicLong max = new AtomicLong(0);
  
  public MetricsNumber() {
  }
  
  public void mark() {
    lastValue.getAndSet(currentValue.get());
  }
  
  public long getDiff() {
    return currentValue.get() - lastValue.get();
  }

  public void add(long delta) {
    if(delta > max.get()) {
      max.getAndSet(delta);
    }
    
    count.incrementAndGet();
    currentValue.addAndGet(delta);
  }
  
  public long getValue() {
    return currentValue.get();
  }
  
  public long getMax() {
    return max.get();
  }
  
  public long getCount() {
    return count.get();
  }
  
  public float getAvg() {
    if(count.get() == 0) {
      return 0;
    }
    return (float)(currentValue.floatValue() / count.floatValue());
  }
}
