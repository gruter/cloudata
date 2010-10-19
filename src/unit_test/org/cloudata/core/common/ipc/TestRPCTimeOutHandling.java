package org.cloudata.core.common.ipc;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.ipc.CRPC;
import org.cloudata.core.common.ipc.CVersionedProtocol;
import org.cloudata.core.common.ipc.CRPC.Server;

import junit.framework.TestCase;


public class TestRPCTimeOutHandling extends TestCase {
  CloudataConf conf = new CloudataConf();

  public void testRPCTimeout() {
    try {
      Server server = CRPC.getServer(null, new TestServer(), "0.0.0.0", 8888, 2, false, conf, null);
      server.start();
      System.out.println("Server starts");
      
      try {
        Thread.sleep(3000);
      } catch (InterruptedException e) {
        e.printStackTrace();
        return;
      }

      final TestServerIF proxy = (TestServerIF) CRPC.getProxyWithoutVersionChecking(TestServerIF.class,
          0l, new InetSocketAddress("127.0.0.1", 8888), conf);

      Thread thread1 = new Thread() {
        public void run() {
          System.out.println("thread1 starts");
          try {
            proxy.doSomething(true);
            assertTrue(false);
          } catch(Exception e) {
            System.out.println("thread 1 timed out");
          }
        }
      };
      
      thread1.start();
      
      try {
        Thread.sleep(1000);
      } catch(Exception e) {
        e.printStackTrace();
        return;
      }
      
      Thread thread2 = new Thread() {
        public void run() {
          try {
            System.out.println("thread2 starts");
            proxy.doSomething(false);
          } catch(Exception e) {
            e.printStackTrace();
            assertTrue(false);
          }
          System.out.println("thread2 ends");
        }
      };
      
      thread2.start();

      try {
        thread1.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      
      Thread thread3 = new Thread() {
        public void run() {
          try {
            System.out.println("thread3 starts");
            proxy.doSomething(false);
          } catch(Exception e) {
            e.printStackTrace();
            assertTrue(false);
          }
          System.out.println("thread3 ends");
        }
      };
      
      thread3.start();

      try {
        thread3.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    
    try {
      Thread.sleep(conf.getInt("ipc.client.timeout", 10000) + 1000);
    } catch(Exception e) {
    }
  }
}

interface TestServerIF extends CVersionedProtocol {
  public void doSomething(boolean doSleep);
}

class TestServer implements TestServerIF {
  public void doSomething(boolean doSleep) {
    System.out.println(Thread.currentThread().getName() + " enters");
    if (doSleep) {
      sleep(20000);
    } else {
      sleep(2000);
    }
    System.out.println(Thread.currentThread().getName() + " exits");
  }
  
  private void sleep(int msec) {
    try {
      Thread.sleep(msec);
    } catch (InterruptedException e) {
    }
  }

  public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
    return 0;
  }
}
