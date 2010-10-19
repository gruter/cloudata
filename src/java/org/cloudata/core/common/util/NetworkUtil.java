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
package org.cloudata.core.common.util;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.mortbay.log.Log;

public class NetworkUtil {
  public static InetSocketAddress getLocalAddress(int port) throws UnknownHostException {
    String hostInfo = null;
    
    try {
      hostInfo = InetAddress.getLocalHost().getHostName();
      InetAddress.getByName(hostInfo);
      return new InetSocketAddress(hostInfo, port);
    } catch (UnknownHostException e) {
    }
    return new InetSocketAddress(InetAddress.getLocalHost().getHostAddress(), port);
  }
  
  public static InetSocketAddress getAddress(String hostStr) {
    String target = hostStr;
    int colonIndex = target.indexOf(':');
    if (colonIndex < 0) {
      throw new RuntimeException("Not a host:port pair: " + hostStr);
    }
    String host = target.substring(0, colonIndex);
    int port = Integer.parseInt(target.substring(colonIndex + 1));

    return new InetSocketAddress(host, port);
  }

//  public static String hostNameToFileName(String hostName) {
//    int index = hostName.indexOf(":");
//    if (index < 0)
//      return hostName;
//
//    return hostName.substring(0, index) + "_" + hostName.substring(index + 1);
//  }
//
//  public static String fileNameToHostName(String fileName) {
//    int index = fileName.lastIndexOf("_");
//    if (index < 0)
//      return fileName;
//
//    return fileName.substring(0, index) + ":" + fileName.substring(index + 1);
//  }
  
  public static String stringifyInetSocketAddressList(InetSocketAddress[] addresses) {
    StringBuilder sb = new StringBuilder();
    
    for(int i = 0; i < addresses.length; i++) {
      sb.append(addresses[i].getAddress()).append(":").append(addresses[i].getPort());
      
      if (i + 1 < addresses.length) {
        sb.append(", ");
      }
    }
    
    return sb.toString();
  }
}
