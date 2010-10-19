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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;

import org.cloudata.core.commitlog.CommitLogClient;
import org.cloudata.core.commitlog.ListenerTestIF;
import org.cloudata.core.commitlog.RpcUtil;
import org.cloudata.core.common.ForkableProcess;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.testhelper.FaultManager;

import junit.framework.TestCase;


public class TestCommitLogServerFailure extends TestCase {
  static final String TEST_STRING = "As recently as the 1990s, studies showed that most people preferred getting information from other people rather than from information retrieval (IR) systems. Of course, in that time period, most people also used human travel agents to book their travel. However, during the last decade, relentless optimization of information retrieval effectiveness has driven web search engines to new quality levels at which most people are satisfied most of the time, and web search has become a standard and often preferred source of information finding. For example, the 2004 Pew Internet Survey (Fallows 2004) found that “92% of Internet users say the Internet is a good place to go for getting everyday information.” To the surprise of many, the field of information retrieval has moved from being a primarily academic discipline to being the basis underlying most people’s preferred means of information access. This book presents the scientific underpinnings of this field, at a level accessible to graduate students as well as advanced undergraduates. Information retrieval did not begin with the Web. In response to various challenges of providing information access, the field of IR evolved to give principled approaches to searching various forms of content. The field began with scientific publications and library records but soon spread to other forms of content, particularly those of information professionals, such as journalists, lawyers, and doctors. Much of the scientific research on IR has occurred in these contexts, and much of the continued practice of IR deals with providing access to unstructured information in various corporate and governmental domains, and this work forms much of the foundation of our book.";
  static final int NUM_THREAD = 12;

  CloudataConf conf = new CloudataConf();
  ForkableProcess[] serverProcessList = new ForkableProcess[3];
  RpcUtil rpcUtil;
  FaultManager fm;

  protected void setUp() throws Exception {
    conf.set("testmode", "true");
    conf.set("cloudata.commitlog.filesystem", "pipe");
    conf.set("cloudata.filesystem", "local");
    conf.set("cloudata.local.temp", System.getProperty("user.home") + File.separator + ".cloudata_local");

    fm = FaultManager.create(conf);
    fm.clearAllFaults();
    
    buildRpcUtil();

    for (int i = 0; i < serverProcessList.length; i++) {
      serverProcessList[i] = new ForkableProcess(
          "org.cloudata.core.commitlog.CommitLogServer", String.valueOf(i), "true");

      final Method method = rpcUtil.buildRpcMethod("stopCommitLogServer");
      final int rpcIndex = i;

      serverProcessList[i].setStopMethod(new Runnable() {
        public void run() {
          try {
            rpcUtil.singleOnewayCall(method, rpcIndex);
          } catch (IOException e) {
          }
        }
      });

      serverProcessList[i].start();
    }

    sleep(10);

    formatCommitLogServers();
  }

  private void formatCommitLogServers() throws IOException {
    rpcUtil.multiParallelCall_v2(rpcUtil.buildRpcMethod("format"));
  }

  protected void tearDown() {
    System.out.println("#### tearDown ####");
    for (int i = 0; i < serverProcessList.length; i++) {
      try {
        serverProcessList[i].stop();
      } catch (Exception e) {
      }
    }
    sleep(5);

    fm.close();
  }

  private void buildRpcUtil() {
    InetSocketAddress[] rpcAddressList = new InetSocketAddress[3];

    for (int i = 0; i < rpcAddressList.length; i++) {
      rpcAddressList[i] = new InetSocketAddress("127.0.0.1", (57001 + i));
    }

    rpcUtil = new RpcUtil(conf, rpcAddressList);
  }

  public void testBasic_100() {
    System.out.println("testBasic_100()");
    CommitLogClient fs = null;
    String tabletName = "SangchulTabletName";

    try {
      fs = new CommitLogClient(conf);
      fs.open();
      fs.startWriting(1, tabletName);
      fs.write(TEST_STRING.getBytes());
      int numWritten = fs.commitWriting();

      assertEquals(numWritten, fs.getLogFileSize(tabletName));
      fs.close();
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue(false);
    }

    if (fs != null)
      try {
        fs.removeAllLogs(tabletName);
      } catch (IOException e) {
      }
  }

  public void testBasic_101() {
    System.out.println("testBasic_101()");
    new Test_101_runnable(0).run();
  }

  public void testBasic_101_mt() {
    System.out.println("testBasic_101_mt()");
    runThreadTest(Test_101_runnable.class);
  }

  public void testBasic_102() {
    System.out.println("testBasic_102()");
    new Test_102_runnable(0).run();
  }

  public void testBasic_102_mt() {
    System.out.println("testBasic_102_mt()");
    runThreadTest(Test_102_runnable.class);
  }

  public void testBasic_102_withDelay() {
    System.out.println("testBasic_102_withDelay()");

    /*
     * Pipe의 timeout 정책을 어떻게 할 것인지 결정한 다음
     * 다시 테스트 코드를 작성해야 한다.
     * 지금 현재 pipe는 120초 timeout을 사용하고 있는데, 이는 너무 길고,
     * 이 값은 configurable한데, CommitLogClient의 timeout과
     * Pipe의 expiration timeout간의 관계에 대해 정리가 필요하다.
     */
  }

  public void testBasic_103() {
    System.out.println("testBasic_103()");
  }

  public void testBasic_104() {
    System.out.println("testBasic_104()");
    serverProcessList[1].stop();
    sleep(1);
    CommitLogClient fs = null;

    try {
      String tabletName = "SangchulTabletName";
      fs = new CommitLogClient(conf);
      fs.open();
      assertTrue(false);
    } catch (IOException e) {
      assertTrue(e.toString().startsWith("java.io.IOException: Fail to establish pipe connection"));
    }
  }

  public void testBasic_105() {
    System.out.println("testBasic_105()");
    CommitLogClient fs = null;

    try {
      fm.inject(ListenerTestIF.class, "handleNewConnection", DeathFault.class);
      String tabletName = "SangchulTabletName";
      fs = new CommitLogClient(conf);
      fs.open();
      assertTrue(false);
    } catch (IOException e) {
      if (e.toString().startsWith("java.io.IOException: Fail to establish pipe connection") == false) {
        e.printStackTrace();
        assertTrue(false);
      }
    } catch (InterruptedException e) {
    }
  }

  public void testBasic_106() {
    System.out.println("testBasic_106()");
    CommitLogClient fs = null;

    try {
      fm.inject(ListenerTestIF.class, "handleNewConnection", DelayFault.class);
      String tabletName = "SangchulTabletName";
      fs = new CommitLogClient(conf);
      fs.open();
      assertTrue(false);
    } catch (java.net.SocketTimeoutException e) {
    } catch (IOException e) {
      if (e.toString().indexOf("Fail to establish pipe connection") < 0) {
        e.printStackTrace();
        assertTrue(false);
      }
    } catch (InterruptedException e) {
    }
  }
  
  private void truncate(String parentDir, String fileName, int size) {
  }

  class Test_101_runnable implements Runnable {
    int num;

    public Test_101_runnable(int i) {
      this.num = i;
    }

    public void run() {
      CommitLogClient fs = null;
      String tabletName = "Sangchul_" + num;
      try {
        int numWritten;
        fs = new CommitLogClient(conf);
        fs.open();
        fs.startWriting(1, tabletName);
        fs.write(TEST_STRING.getBytes());
        numWritten = fs.commitWriting();
        fs.close();
        assertEquals(numWritten, fs.getLogFileSize(tabletName));

        numWritten = appendWithOpenClose(fs, numWritten, tabletName);
        numWritten = appendWithOpenClose(fs, numWritten, tabletName);
        numWritten = appendWithOpenClose(fs, numWritten, tabletName);

      } catch (Exception e) {
        e.printStackTrace();
        assertTrue(false);
      }

      if (fs != null)
        try {
          fs.removeAllLogs(tabletName);
        } catch (IOException e) {
        }
    }
  };

  class Test_102_runnable implements Runnable {
    int num;
    int sleepSec = 0;

    public Test_102_runnable(int i) {
      this.num = i;
    }

    public void setDelayBeforeAppend(int sec) {
      this.sleepSec = sec;
    }

    public void run() {
      CommitLogClient fs = null;
      String tabletName = "Sangchul_" + num;
      try {
        int numWritten;
        fs = new CommitLogClient(conf);
        fs.open();
        fs.startWriting(1, tabletName);
        fs.write(TEST_STRING.getBytes());
        numWritten = fs.commitWriting();
        assertEquals(numWritten, fs.getLogFileSize(tabletName));

        sleep(sleepSec);
        numWritten = appendWithoutOpenClose(fs, numWritten, tabletName);
        sleep(sleepSec);
        numWritten = appendWithoutOpenClose(fs, numWritten, tabletName);
        sleep(sleepSec);
        numWritten = appendWithoutOpenClose(fs, numWritten, tabletName);

        fs.close();
      } catch (IOException e) {
        e.printStackTrace();
        assertTrue(false);
      }

      if (fs != null) {
        try {
          fs.removeAllLogs(tabletName);
        } catch (IOException e) {
        }
      }
    }
  };

  private int appendWithOpenClose(CommitLogClient fs, int numWritten, String tabletName)
      throws IOException {
    sleep(1);

    fs.open();
    fs.startWriting(1, tabletName);
    fs.write(TEST_STRING.getBytes());
    numWritten += fs.commitWriting();
    fs.close();

    assertEquals(numWritten, fs.getLogFileSize(tabletName));
    return numWritten;
  }

  private int appendWithoutOpenClose(CommitLogClient fs, int numWritten, String tabletName)
      throws IOException {
    sleep(1);

    fs.startWriting(1, tabletName);
    fs.write(TEST_STRING.getBytes());
    numWritten += fs.commitWriting();

    assertEquals(numWritten, fs.getLogFileSize(tabletName));
    return numWritten;
  }

  private void sleep(int sec) {
    if (sec > 0) {
      System.out.println("#### SLEEP " + sec + " sec ####");
      try {
        Thread.sleep(sec * 1000);
      } catch (Exception e) {
      }
    }
  }

  private void runThreadTest(Class<? extends Runnable> runnableClass) {
    Thread[] threadList = new Thread[NUM_THREAD];

    try {
      Constructor<? extends Runnable> cons = runnableClass.getConstructor(this.getClass(),
          int.class);
      for (int i = 0; i < threadList.length; i++) {
        threadList[i] = new Thread(cons.newInstance(this, i));
        threadList[i].start();
      }
    } catch (Exception e) {
      e.printStackTrace();
      assertTrue(false);
    }

    for (int i = 0; i < threadList.length; i++) {
      try {
        threadList[i].join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public static void main(String[] args) {
    junit.textui.TestRunner.run(TestCommitLogServerFailure.class);
  }
}

class FaultConstants {
  public static final int FAULT_SERVER_PORT = 18001;
}
