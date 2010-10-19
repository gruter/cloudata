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

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.ZooKeeper;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.exception.TestCaseException;
import org.cloudata.core.common.lock.LockUtil;
import org.cloudata.core.master.CloudataMaster;


public class TestHandler  extends DefaultInvocationHandler {
  public static final Log LOG = LogFactory.getLog(CloudataMaster.class.getName());
  
  public static final String TEST_LOCK_PATH = "/test";
  
  private ZooKeeper zk;
  
  private static Map<String, Map<String, UnitTestDetail>> unitTestDetails = new HashMap<String, Map<String, UnitTestDetail>>();
  
  private Object monitor = new Object();
  
  private Random rand = new Random();
  
  private Method currentMethod;
  
  private String lockOwner;
  
  private CloudataConf conf;
  
  public TestHandler() throws IOException {
    conf = new CloudataConf();
    lockOwner = "TestHandler_" + String.valueOf(System.nanoTime() + rand.nextInt());
    zk = LockUtil.getZooKeeper(conf, lockOwner, null);
    
    //FIXME Test를 위해서는 Test target class가 Test 대상 객체가 event 발생 전에 미리 생성되어 있어야 한다.
    //TabletDropAction의 경우 drop() 이벤트가 발생해야지만 생성되지만 이미 테스트 관련 이벤트는 생성되었기 때문에
    //TestLockEventProcessor가 수행되지 않는다.
    
//    lockService.getEventHandler().addLockEventMonitor(new TestLockEventMonitor());
  }
  
  public void setTargetObject(ProxyObject targetObject) {
    super.setTargetObject(targetObject);
//    if(targetObject instanceof TabletServer) {
//      System.out.println("TestHandler created:lockOwner=" + lockOwner + ", targetObject=" + targetObject + ">" + this);
//    }
  }
  
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    UnitTestDetail unitTestDetail = null;
    currentMethod = method;
    String methodName = method.getName();
    if(!"getTestHandlerKey".equals(methodName) && !"init".equals(methodName)) {
      synchronized(unitTestDetails) {
        Map<String, UnitTestDetail> testMethods = unitTestDetails.get(targetObject.getTestHandlerKey());
        if(testMethods != null) {
          unitTestDetail = testMethods.get(methodName);
        }
//        if("dropTable".equals(methodName)) {
//          System.out.println("====Call Start:" + targetObject.getTestHandlerKey() + "," + methodName + 
//              ",unitTestDetail=" + unitTestDetail + ",testMethods=" + testMethods);
//        }
        if(hasUnitTest(unitTestDetail, true)) {
          boolean continueYn = unitTestDetail.getTestProcessor().runTest(this, unitTestDetail);
          unitTestDetail.setAlreadyRun(true);
          if(!continueYn) {
            return null;
          }
        }
      }
    }
    Object result = method.invoke(targetObject, args);
    
    if(hasUnitTest(unitTestDetail, false)) {
      boolean continueYn = unitTestDetail.getTestProcessor().runTest(this, unitTestDetail);
      unitTestDetail.setAlreadyRun(true);
      //System.out.println("====Call End:" + method.getName() + "continueYn=" + continueYn + ",unitTestDetail=" + unitTestDetail);
      if(!continueYn) {
        throw new TestCaseException(method.getName());
      }
    }
    return result;
  }
  
  public ZooKeeper getZooKeeper() {
    return zk;
  }
  
  public String getLockOwner() {
    return lockOwner;
  }
  
  public void removeCurrentTestMethod() {
    //System.out.println("Remove Test Case1:targetObject=" + targetObject.getTestHandlerKey() + ",methodName=" + currentMethod.getName());
    synchronized(unitTestDetails) {
      Map<String, UnitTestDetail> testMethods = unitTestDetails.get(targetObject.getTestHandlerKey());
      if(testMethods == null) {
        return;
      }
      synchronized(testMethods) {
        testMethods.remove(currentMethod.getName());
      }
      String lockId = TestHandler.TEST_LOCK_PATH + "/" + currentMethod.getName();
      try {
        LockUtil.delete(zk, LockUtil.getZKPath(conf, lockId), true);
      } catch (Exception e) {
        LOG.error("fail to delete node lockID[" + lockId + "]");
      }
    }
  }
  
  private boolean hasUnitTest(UnitTestDetail unitTestDetail, boolean before) {
    return unitTestDetail != null && 
              unitTestDetail.getTargetClass().equals(targetObject.getClass().getName()) && 
              unitTestDetail.isBefore() == before ;
  }
  
  public static void clear() {
    unitTestDetails.clear();
  }
}
