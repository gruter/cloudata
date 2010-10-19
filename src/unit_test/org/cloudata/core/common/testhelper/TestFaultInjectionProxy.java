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
package org.cloudata.core.common.testhelper;

import java.io.IOException;

import org.cloudata.core.common.ForkableProcess;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.testhelper.Fault;
import org.cloudata.core.common.testhelper.FaultException;
import org.cloudata.core.common.testhelper.FaultInjectionProxy;
import org.cloudata.core.common.testhelper.FaultManager;

import junit.framework.TestCase;


public class TestFaultInjectionProxy extends TestCase {
  FaultManager fm;
  
  protected void setUp() throws Exception {
    fm = FaultManager.create(new CloudataConf());
    
    TestFault.beforeInvoked = false;
    TestFault.afterInvoked = false;
  }

  protected void tearDown() throws Exception {
    fm.close();
  }
  
  public void testCreateProxyWithoutInjection() {
    MyTestClass body = new MyTestClass();
    MyTestClassIF proxy = FaultInjectionProxy.wrap(body, MyTestClassIF.class);
    String msg = "Hello world";
    proxy.setStatus(msg);
    assertEquals(msg, body.status);
  }

  public void testCreateProxyWithInjection() {
    MyTestClass body = new MyTestClass();
    
    injectFault(MyTestClassIF.class, "setStatus", TestFault.class);
    
    String msg = "Hello world";

    MyTestClassIF proxy = FaultInjectionProxy.wrap(body, MyTestClassIF.class);
    
    proxy.setStatus(msg);
    
    assertEquals(msg, body.status);
    assertEquals(true, TestFault.beforeInvoked);
    assertEquals(true, TestFault.afterInvoked);
    
    TestFault.beforeInvoked = false;
    TestFault.afterInvoked = false;
    body.status = "";
    
    clearFault(MyTestClassIF.class, "setStatus");
    
    proxy.setStatus(msg);
    
    assertEquals(msg, body.status);
    assertEquals(false, TestFault.beforeInvoked);
    assertEquals(false, TestFault.afterInvoked);
  }

  // by sangchul
  // proxy로 감싼 인터페이스로 fault가 injection된게 아닌,
  // 그 인터페이스의 부모 인터페이스 이름으로 injection된 fault도 play되어야 한다.
  public void testCreateProxyWithInterfaceHierarchy() {
    MyChildTestClass body = new MyChildTestClass();
    
    injectFault(MyTestClassIF.class, "setStatus", TestFault.class);
    
    String msg = "Hello world";

    MyTestClassIF proxy = FaultInjectionProxy.wrap(body, ChildTestIF.class);
    
    proxy.setStatus(msg);
    
    assertEquals(msg, body.status);
    assertEquals(true, TestFault.beforeInvoked);
    assertEquals(true, TestFault.afterInvoked);
    
  }
  
  // by sangchul
  // fork 시킨 프로세스에서 작동하는 클래스에도 fault가 injection되어야 한다.
  public void testProxyInForkedProcess() {
    try {
      fm.inject(MyTestClassIF.class, "setStatus", InterProcessFault.class);
    } catch (IOException e1) {
      e1.printStackTrace();
      assertTrue(false);
    } catch (InterruptedException e) {
      e.printStackTrace();
      assertTrue(false);
    }

    ForkableProcess process 
      = new ForkableProcess("org.cloudata.core.common.testhelper.ForkedProcess");
    process.start();
    
    try {
      process.waitForExit();
    } catch (InterruptedException e) {
    }
    
    assertEquals(2, process.getExitValue());
  }
  
  public void testProxyWithCondition() {
    MyTestClass body1 = new MyTestClass();
    body1.status = "moon";
    MyTestClass body2 = new MyTestClass();
    body2.status = "sangchul";
    
    injectFault(MyTestClassIF.class, "setStatus", ConditionalFault.class);
    
    String msg = "zzang";

    MyTestClassIF proxy1 = FaultInjectionProxy.wrap(body1, MyTestClassIF.class);
    MyTestClassIF proxy2 = FaultInjectionProxy.wrap(body2, MyTestClassIF.class);
    
    try {
      proxy1.setStatus(msg);
    } catch(FaultException e) {
    }
    
    try {
      proxy2.setStatus(msg);
    } catch(FaultException e) {
    }
    
    assertEquals("moon", body1.status);
    assertEquals("zzang", body2.status);
  }
  
  //////////////////////// private methods ////////////////////////////////
  
  private void injectFault(Class<?> c, String methodName, Class<? extends Fault> faultClass) {
    try {
      fm.inject(c, methodName, faultClass);
      sleep(2);
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue(false);
    } catch (InterruptedException e) {
      e.printStackTrace();
      assertTrue(false);
    }
  }
  
  private void clearFault(Class<?> c, String methodName) {
    try {
      fm.clear(c, methodName);
      sleep(2);
    } catch(IOException e) {
      e.printStackTrace();
      assertTrue(false);
    } catch (InterruptedException e) {
      e.printStackTrace();
      assertTrue(false);
    }
  }
  
  private void sleep(int sec) {
    try {
      Thread.sleep(sec * 1000);
    } catch(Exception e) {
    }
  }
}

interface MyTestClassIF {
  public void setStatus(String status);
}

class MyTestClass implements MyTestClassIF {
  String status = null;
  
  public void setStatus(String status) {
    this.status = status;
  }
}

interface ChildTestIF extends MyTestClassIF {
  
}

class MyChildTestClass extends MyTestClass implements ChildTestIF {
}

class TestFault extends Fault {
  static boolean afterInvoked = false;
  static boolean beforeInvoked = false;

  public void playAfterInvoke(Object body) {
    afterInvoked = true;      
  }

  public void playBeforeInvoke(Object body) {
    beforeInvoked = true;
  }    
}

class InterProcessFault extends Fault {
  public void playAfterInvoke(Object body) {
    ForkedProcess.afterInvokeCalled = true;      
  }

  public void playBeforeInvoke(Object body) {
    ForkedProcess.beforeInvokeCalled = true;
  }    
}

class ConditionalFault extends Fault {
  public void playAfterInvoke(Object body) {

  }

  public void playBeforeInvoke(Object body) {
    MyTestClass obj = (MyTestClass) body;
    if (obj == null) {
      System.out.println("obj is null");  
    } else if (obj.status == null) {
      System.out.println("obj.status is null");
    }

    if (obj.status.equals("moon")) {
      throw new FaultException();
    }  
  }
}

class ForkedProcess {
  static boolean afterInvokeCalled = false;
  static boolean beforeInvokeCalled = false;
  
  public static void main(String[] args) {
    System.out.println("begin process");
    MyTestClass body = new MyTestClass();
    MyTestClassIF proxy = FaultInjectionProxy.wrap(body, MyTestClassIF.class);
    String msg = "Hello world";
    
    proxy.setStatus(msg);

    System.out.println("end process");
    System.exit((afterInvokeCalled && beforeInvokeCalled) ? 2 : 1);
  }
}
