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
package org.cloudata.core.common.aop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.cloudata.core.common.io.CWritable;
import org.cloudata.core.common.io.CWritableUtils;


public class UnitTestDetail implements CWritable {
  /**
   * 테스트 수행 대상 클래스
   */
  private String targetClass;
  
  /**
   * 테스트 수행 대상 메소드
   */
  private String methodName;
  
  /**
   * 테스트 수행 대상 메소드가 호출되었을 때 수행해야 하는 테스트 코드
   */
  DistributedUnitTestProcessor testProcessor;
  
  /**
   * 테스트 수행 대상 메소드 호출 전 또는 호출 후 여부
   */
  boolean before;

  /**
   * 하나의 테스트 대상 클래스에 대해 여러 객체가 수행되는 경우(TabletServer) 
   * 하나의 객체만 대상으로 할 것인지 여부
   */
  boolean onlyOneTarget;
  
  
  boolean alreadyRun = false;
  
  public UnitTestDetail() {
    
  }
  
  public UnitTestDetail(String lockContents) throws IOException {
    String[] lockInfos = lockContents.split(",");
    setTargetClass(lockInfos[0].trim());
    setMethodName(lockInfos[1].trim());
    setTestProcessorClassName(lockInfos[2].trim());
    setBefore("true".equals(lockInfos[3].trim()));
    if(lockInfos.length > 4) {
      setOnlyOneTarget("true".equals(lockInfos[4].trim()));
    }
  }
  
  public void readFields(DataInput in) throws IOException {
    methodName = CWritableUtils.readString(in);
    String testProcessorClassName = CWritableUtils.readString(in);
    setTestProcessorClassName(testProcessorClassName);
    before = in.readBoolean();
    onlyOneTarget = in.readBoolean();
  }

  public void write(DataOutput out) throws IOException {
    CWritableUtils.writeString(out, methodName);
    CWritableUtils.writeString(out, testProcessor.getClass().getName());
    out.writeBoolean(before);
    out.writeBoolean(onlyOneTarget);
  }


  public void setTestProcessorClassName(String className) throws IOException{
    try {
      testProcessor = (DistributedUnitTestProcessor)Class.forName(className).newInstance();
    } catch (Exception e) {
      throw new IOException(e.getMessage());
    }
  }
  
  public boolean isBefore() {
    return before;
  }

  public void setBefore(boolean before) {
    this.before = before;
  }

  public String getMethodName() {
    return methodName;
  }

  public void setMethodName(String methodName) {
    this.methodName = methodName;
  }

  public DistributedUnitTestProcessor getTestProcessor() {
    return testProcessor;
  }

  public void setTestProcessor(DistributedUnitTestProcessor testProcessor) {
    this.testProcessor = testProcessor;
  }

  public String getTargetClass() {
    return targetClass;
  }

  public void setTargetClass(String targetClass) {
    this.targetClass = targetClass;
  }  
  
  public String toString() {
    return targetClass + "," + methodName + "," + testProcessor.getClass().getName() + "," + before;
  }

  public boolean isOnlyOneTarget() {
    return onlyOneTarget;
  }

  public void setOnlyOneTarget(boolean onlyOneTarget) {
    this.onlyOneTarget = onlyOneTarget;
  }

  public boolean isAlreadyRun() {
    return alreadyRun;
  }

  public void setAlreadyRun(boolean alreadyRun) {
    this.alreadyRun = alreadyRun;
  }
}
