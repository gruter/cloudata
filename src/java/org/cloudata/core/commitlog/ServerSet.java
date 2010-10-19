package org.cloudata.core.commitlog;

import java.net.InetSocketAddress;

public class ServerSet {
  int hash;
  private final InetSocketAddress[] addrList;

  ServerSet(InetSocketAddress[] addrList) {
    this.addrList = addrList;
    for(InetSocketAddress addr : addrList) {
      hash ^= addr.hashCode();
    }
  }

  public boolean equals(Object obj) {
    if (obj instanceof ServerSet == false) {
      return false;
    }
    
    ServerSet set = (ServerSet) obj;
    if (set.addrList.length != this.addrList.length) {
      return false;
    }
    
    for(int i = 0; i < this.addrList.length; i++) {
      if (addrList[i].equals(set.addrList[i]) == false) {
        return false;
      }
    }
    
    return true;
  }
  
  public String toString() {
    String ret = "";
    for(InetSocketAddress addr : addrList) {
      ret += addr + ", ";
    }
    
    return ret;
  }

  public int hashCode() {
    return hash;
  }

  public InetSocketAddress[] getAddressList() {
    return addrList;
  }
}
