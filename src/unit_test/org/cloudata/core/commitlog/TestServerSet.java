package org.cloudata.core.commitlog;

import java.net.InetSocketAddress;
import java.util.HashMap;

import org.cloudata.core.commitlog.ServerSet;

import junit.framework.TestCase;

public class TestServerSet extends TestCase {
  public void testEquals() {
    InetSocketAddress[] addrList1 = { InetSocketAddress.createUnresolved("127.0.0.1", 12345),
        InetSocketAddress.createUnresolved("127.0.0.2", 12345),
        InetSocketAddress.createUnresolved("127.0.0.3", 12345) };

    InetSocketAddress[] addrList2 = { InetSocketAddress.createUnresolved("127.0.0.2", 12345),
        InetSocketAddress.createUnresolved("127.0.0.1", 12345),
        InetSocketAddress.createUnresolved("127.0.0.3", 12345) };

    assertEquals(new ServerSet(addrList1), new ServerSet(addrList1));
    assertNotSame(new ServerSet(addrList1), new ServerSet(addrList2));
  }

  public void testHashCode() {
    InetSocketAddress[] addrList1 = {
        InetSocketAddress.createUnresolved("127.0.0.1", 12345),  
        InetSocketAddress.createUnresolved("127.0.0.2", 12345),  
        InetSocketAddress.createUnresolved("127.0.0.3", 12345)  
      };

    InetSocketAddress[] addrList2 = {
        InetSocketAddress.createUnresolved("127.0.0.1", 12345),  
        InetSocketAddress.createUnresolved("127.0.0.2", 12345),  
        InetSocketAddress.createUnresolved("127.0.0.3", 12345)  
      };

    assertNotSame(addrList1.hashCode(), addrList2.hashCode());
    assertEquals(new ServerSet(addrList1).hashCode(), new ServerSet(addrList2).hashCode()); 
    
    HashMap<ServerSet, String> map = new HashMap<ServerSet, String>();
    map.put(new ServerSet(addrList1), "OK");
    assertNotNull(map.get(new ServerSet(addrList2)));
  }
}
