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

import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.testhelper.Fault;
import org.cloudata.core.common.testhelper.FaultManager;
import org.cloudata.core.common.testhelper.FaultManager.FaultHelper;
import org.cloudata.core.common.testhelper.FaultManager.FaultHelper.InjectionInfo;

import junit.framework.TestCase;


public class TestFaultManager extends TestCase {
  CloudataConf conf = new CloudataConf(); 
  
  static final String methodName = "testMethod";
  static final String contents = "org.cloudata.core.common.testhelper.TestFalultHelper" 
    + FaultHelper.DELIMINATOR
    + methodName 
    + FaultHelper.DELIMINATOR 
    + "org.cloudata.core.common.testhelper.MyFault";


  public void testFaultManagerInstance() {
    try {
      FaultManager fm = FaultManager.create(conf);
      assertNotNull(fm.zk);
      FaultManager fm2 = FaultManager.create(conf);
      
      assertTrue(fm != fm2);
      
      fm.close();
      assertNull(fm.zk);
      
      try {
        fm.inject(this.getClass(), "dummyMethod", null);
        assertTrue(false);
      } catch(IOException e) {
      } catch (InterruptedException e) {
        assertTrue(false);
      }
      
      fm2.close();
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue(false);
    }
  }
  
  public void testCreateLockPath() {
    assertEquals(
        "org_cloudata_core_common_testhelper_TestFaultManager/myMethod",
        FaultManager.FaultHelper.createLockPath(this.getClass(), "myMethod"));
  }
  
  public void testInjectClearAndGetFault() {
    FaultManager fm = null;

    try {
      fm = FaultManager.create(conf);
      
      fm.inject(this.getClass(), "testInjectAndGetFault", TestFault.class);
      
      Fault fault = fm.getFault(this.getClass(), "testInjectAndGetFault");
      
      assertNotNull(fault);
      assertEquals(TestFault.class, fault.getClass());
      
      fm.clear(this.getClass(), "testInjectAndGetFault");
      
      fault = fm.getFault(this.getClass(), "testInjectAndGetFault");
      
      assertNull(fault);
    } catch (InterruptedException e) {
      assertTrue(false);
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue(false);
    } finally {
      if (fm != null) {
        fm.close();
      }
    }
  }
  
  public void testInjectClearAndGetFaultInDifferentFM() {
    FaultManager fm1 = null;
    FaultManager fm2 = null;

    try {
      fm1 = FaultManager.create(conf);
      
      fm1.inject(this.getClass(), "testInjectAndGetFault", TestFault.class);
      
      Fault fault = fm1.getFault(this.getClass(), "testInjectAndGetFault");
      
      assertNotNull(fault);
      assertEquals(TestFault.class, fault.getClass());
      
      fm2 = FaultManager.create(conf);
      Fault fault2 = fm2.getFault(this.getClass(), "testInjectAndGetFault");
      assertNotNull(fault2);
      
      fm2.clear(this.getClass(), "testInjectAndGetFault");

      Thread.sleep(1000);
      
      assertNull(fm1.getFault(this.getClass(), "testInjectAndGetFault"));
      assertNull(fm2.getFault(this.getClass(), "testInjectAndGetFault"));
    } catch (InterruptedException e) {
      assertTrue(false);
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue(false);
    } finally {
      if (fm1 != null) {
        fm1.close();
      }
      
      if (fm2 != null) {
        fm2.close();
      }
    }
  }
  
  public void testOverwriteFaults() {
    FaultManager fm = null;

    try {
      fm = FaultManager.create(conf);
      
      fm.inject(this.getClass(), "testInjectAndGetFault", TestFault.class);
      fm.inject(this.getClass(), "testInjectAndGetFault", TestFault2.class);
      
      Fault fault = fm.getFault(this.getClass(), "testInjectAndGetFault");
      
      assertEquals(TestFault2.class, fault.getClass());
      
      fm.clear(this.getClass(), "testInjectAndGetFault");
      
      assertNull(fm.getFault(this.getClass(), "testInjectAndGetFault"));
    } catch (InterruptedException e) {
      assertTrue(false);
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue(false);
    } finally {
      if (fm != null) {
        fm.close();
      }
    }
  }
  
  public void testShareWithOtherFaultManager() {
    FaultManager fm1 = null;
    FaultManager fm2 = null;
    
    try {
      fm1 = FaultManager.create(conf);
      
      fm1.inject(this.getClass(), "testShareWithOtherFaultManager", TestFault.class);

      // sleep을 할 필요가 없다. 왜냐하면, FaultManager가 create되면서
      // Pleiades의 모든 fault 정보를 Load하기 때문이다. 
      fm2 = FaultManager.create(conf);
      Fault fault = fm2.getFault(this.getClass(), "testShareWithOtherFaultManager");
      assertNotNull(fault);
      assertEquals(TestFault.class, fault.getClass());

      assertNull(fm1.getFault(TestFault.class, "methodHaha"));

      fm2.inject(TestFault.class, "methodHaha", TestFault2.class);
      
      Thread.sleep(1000);

      fault = fm1.getFault(TestFault.class, "methodHaha");
      assertNotNull(fault);
      assertEquals(TestFault2.class, fault.getClass());
    } catch (InterruptedException e) {
      assertTrue(false);
    } catch(IOException e) {
      e.printStackTrace();
      assertTrue(false);
    } finally {
      if (fm1 != null) {
        fm1.close();
      }
      
      if (fm2 != null) {
        fm2.close();
      }
    }
  }

  public void testDecoding() {
    try {
      InjectionInfo info 
        = FaultHelper.convertAsInfo(
            "org_cloudata_core_common_testhelper_TestFaultManager/" 
              + methodName, 
            "org_cloudata_core_common_testhelper_TestFaultManager$TestFault");
      
      assertEquals(this.getClass(), info.c);
      assertEquals(methodName, info.methodName);
      assertEquals(TestFault.class, info.fault.getClass());
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue(false);
    }
  }
  
  public void testClearAllFaults() {
    FaultManager fm1 = null;
    FaultManager fm2 = null;
    
    try {
      fm1 = FaultManager.create(conf);
      fm1.inject(this.getClass(), "testShareWithOtherFaultManager", TestFault.class);
      fm2 = FaultManager.create(conf);
      fm2.clearAllFaults();
      
      Thread.sleep(1000);

      assertNull(fm1.getFault(this.getClass(), "testShareWithOtherFaultManager"));
    } catch (InterruptedException e) {
      assertTrue(false);
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue(false);
    }

  }
  
  static class TestFault extends Fault {
    public void playAfterInvoke(Object body) {
    }

    public void playBeforeInvoke(Object body) {
    }
  }
  
  static class TestFault2 extends Fault {
    public void playAfterInvoke(Object body) {
    }

    public void playBeforeInvoke(Object body) {
    }
  }  
}
