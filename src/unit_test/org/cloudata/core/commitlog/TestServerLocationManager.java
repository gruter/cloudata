package org.cloudata.core.commitlog;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Comparator;

import junit.framework.TestCase;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.cloudata.core.commitlog.ServerLocationManager;
import org.cloudata.core.commitlog.RandomStrategy.InetSocketAddressComparator;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.exception.CommitLogException;
import org.cloudata.core.common.lock.LockUtil;


public class TestServerLocationManager extends TestCase {
  CloudataConf conf;
  ZooKeeper zk;
  ServerLocationManager manager;

  public TestServerLocationManager() {
    conf = new CloudataConf();
    conf.set("testmode", true);
    conf.set("cloudata.filesystem", "local");
    conf.set("cloudata.commitlog.filesystem", "pipe");
    conf.set("cloudata.local.temp", System.getProperty("user.home") + File.separator + ".cloudata_local");

    try {
      zk = LockUtil.getZooKeeper(conf, "localhost", null);
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue(false);
    }
  }


  protected void setUp() throws Exception {
    try {
      Thread.sleep(1000);
    } catch(Exception e) {
    }
    System.out.println("==== set up ====");
    LockUtil.delete(zk, LockUtil.getZKPath(conf, Constants.COMMITLOG_SERVER), true);
    try {
      Thread.sleep(1000);
    } catch(Exception e) {
    }
  }
  
  protected void tearDown() throws Exception {

    System.out.println("==== tear down ====");
  }

  public void testGetAddresses() {
    ArrayList<String> addrList = new ArrayList<String>();

    addrList.add("10.8.115.126");
    addrList.add("10.8.115.127");
    addrList.add("10.8.115.128");

    for (String addr : addrList) {
      createNode(addr + ":8000");
    }

    try {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }

      manager = ServerLocationManager.instance(conf, null, zk, null);

      assertTrue(3 <= manager.getAllAddresses().length);

      InetSocketAddress[] list = manager.listManager.electCommitLogServers();

      for (String addr : addrList) {
        int i = 0;
        for(; i < list.length; i++) {
          if (list[i].getAddress().getHostName().equals(addr)) {
            break;
          }
          
          assertTrue(i != list.length);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue(false);
    } finally {
      for (String addr : addrList) {
        deleteNode(addr + ":8000");
      }

      manager.clear();
    }
  }

  public void testGetAddressesWithLocal() {
    ArrayList<String> addrList = new ArrayList<String>();

    try {
      addrList.add(InetAddress.getLocalHost().getHostAddress());
    } catch (UnknownHostException e1) {
      e1.printStackTrace();
      assertTrue(false);
    }
    addrList.add("10.8.115.127");
    addrList.add("10.8.115.124");

    for (String addr : addrList) {
      createNode(addr + ":8000");
    }

    try {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }

      manager = ServerLocationManager.instance(conf, null, zk, null);
     
      assertEquals(3, manager.getAllAddresses().length);

      InetSocketAddress[] list = manager.listManager.electCommitLogServers();

      assertEquals(InetAddress.getLocalHost(), list[0].getAddress());
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue(false);
    } finally {
      for (String addr : addrList) {
        deleteNode(addr + ":8000");
      }
      manager.clear();
    }
  }
  
  public void testInsufficientCommitLogServers() {
    try {
      manager = ServerLocationManager.instance(conf, null, zk, null);
      assertTrue(false);
    } catch (CommitLogException e) {
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue(false);
    }
    
    ArrayList<String> addrList = new ArrayList<String>();

    addrList.add("10.8.115.127");
    addrList.add("10.8.115.124");

    for (String addr : addrList) {
      createNode(addr + ":8000");
    }
    
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
    }
    
    try {
      manager = ServerLocationManager.instance(conf, null, zk, null);
      assertTrue(false);
    } catch (CommitLogException e) {
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue(false);
    } finally {
      for (String addr : addrList) {
        deleteNode(addr + ":8000");
      }
    }
  }

  public void testNodeAdded() {

    ArrayList<String> addrList = new ArrayList<String>();

    addrList.add("10.8.115.123");
    addrList.add("10.8.115.127");
    addrList.add("10.8.115.124");

    for (String addr : addrList) {
      createNode(addr + ":8000");
    }
    
    try {
      Thread.sleep(1000);
      manager = ServerLocationManager.instance(conf, null, zk, null);
      
      assertEquals(addrList.size(), manager.getAllAddresses().length);
      
      try {
        createNode(InetAddress.getLocalHost().getHostAddress() + ":8000");
        addrList.add(InetAddress.getLocalHost().getHostAddress());
      } catch (UnknownHostException e1) {
        e1.printStackTrace();
        assertTrue(false);
      }
      
      Thread.sleep(1000);
      
      assertEquals(addrList.size(), manager.getAllAddresses().length);
      assertEquals(InetAddress.getLocalHost()
          , manager.listManager.electCommitLogServers()[0].getAddress());
    } catch (InterruptedException e) {
      e.printStackTrace();
      assertTrue(false);
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue(false);
    } finally {
      for (String addr : addrList) {
        deleteNode(addr + ":8000");
      }
    }
  }

  private void createNode(String hostName) {
    String lockPath = Constants.COMMITLOG_SERVER + "/" + hostName;
    try {
      try {
        if (zk.exists(LockUtil.getZKPath(conf, lockPath), false) != null) {
          LockUtil.delete(zk, LockUtil.getZKPath(conf, lockPath));
        }
      } catch (Exception e) {
        e.printStackTrace();
        assertTrue(false);
      }
      
      LockUtil.createNodes(zk, LockUtil.getZKPath(conf, lockPath), null, CreateMode.EPHEMERAL);
    } catch (Exception e) {
      e.printStackTrace();
      assertTrue(false);
    }
  }

  private void deleteNode(String hostName) {
    String lockPath = Constants.COMMITLOG_SERVER + "/" + hostName;
    try {
      LockUtil.delete(zk, LockUtil.getZKPath(conf, lockPath));
    } catch (Exception e) {
      e.printStackTrace();
      assertTrue(false);
    }
  }
  
  public void testInetSocketAddressComparator() {
    Comparator<InetSocketAddress> comparator = new InetSocketAddressComparator();
    System.out.println("1");
    assertEquals(0, comparator.compare(new InetSocketAddress("127.0.0.1", 1024), new InetSocketAddress("127.0.0.1", 1024)));
    System.out.println("2");
    assertTrue(0 > comparator.compare(new InetSocketAddress("127.0.0.1", 1023), new InetSocketAddress("127.0.0.1", 1024)));
    System.out.println("3");
    assertTrue(0 < comparator.compare(new InetSocketAddress("127.0.0.1", 1024), new InetSocketAddress("127.0.0.1", 1023)));
    System.out.println("4");
    
    assertTrue(0 > comparator.compare(new InetSocketAddress("127.0.0.1", 1024), new InetSocketAddress("127.0.0.2", 1024)));
    System.out.println("5");
    assertTrue(0 < comparator.compare(new InetSocketAddress("127.0.0.2", 1024), new InetSocketAddress("127.0.0.1", 1024)));
    System.out.println("6");
    
    assertTrue(0 < comparator.compare(new InetSocketAddress("127.0.0.2", 1023), new InetSocketAddress("127.0.0.1", 1024)));
    System.out.println("7");
    assertTrue(0 > comparator.compare(new InetSocketAddress("127.0.0.1", 1024), new InetSocketAddress("127.0.0.2", 1023)));
    System.out.println("8");
  }
  
  public void testSequenceStrategy() {
    try {
      ServerListManagementStrategy strategy = new SequenceStrategy(null, 3, 1024);
      
      strategy.addCommitLogServers("127.0.0.1:1024");
      strategy.addCommitLogServers("127.0.0.3:1024");
      strategy.addCommitLogServers("127.0.0.4:1024");
      strategy.addCommitLogServers("127.0.0.5:1024");
      strategy.addCommitLogServers("127.0.0.6:1024");
      strategy.addCommitLogServers("127.0.0.7:1024");
      
      // case 1
      TEST(strategy, new InetSocketAddress("127.0.0.3", 1024)
        , new InetSocketAddress[] {
            new InetSocketAddress("127.0.0.3", 1024), 
            new InetSocketAddress("127.0.0.4", 1024), 
            new InetSocketAddress("127.0.0.5", 1024)
          }
      );

      // case 2
      TEST(strategy, new InetSocketAddress("127.0.0.6", 1024)
        , new InetSocketAddress[] {
            new InetSocketAddress("127.0.0.6", 1024), 
            new InetSocketAddress("127.0.0.7", 1024), 
            new InetSocketAddress("127.0.0.1", 1024)
          }
      );

      
      // case 3
      TEST(strategy, new InetSocketAddress("127.0.0.2", 1024)
        , new InetSocketAddress[] {
            new InetSocketAddress("127.0.0.3", 1024), 
            new InetSocketAddress("127.0.0.4", 1024), 
            new InetSocketAddress("127.0.0.5", 1024)
          }
      );
      
      // case 4
      TEST(strategy, new InetSocketAddress("127.0.0.8", 1024)
        , new InetSocketAddress[] {
            new InetSocketAddress("127.0.0.1", 1024), 
            new InetSocketAddress("127.0.0.3", 1024), 
            new InetSocketAddress("127.0.0.4", 1024)
          }
      );
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }
  
  private void TEST(ServerListManagementStrategy strategy, InetSocketAddress local, InetSocketAddress[] expected) throws IOException {
    strategy.localCommitLogServerAddress = local;
    
    InetSocketAddress[] addrs = strategy.electCommitLogServers();
    
    assertEquals(expected.length, addrs.length);
    
    for(int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], addrs[i]);
    }
  }
}
