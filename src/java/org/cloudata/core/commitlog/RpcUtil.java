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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.ipc.CRPC;


public class RpcUtil {
  CloudataConf conf;
  InetSocketAddress[] rpcAddressList;

  public RpcUtil(CloudataConf conf, InetSocketAddress[] rpcAddressList) {
    this.conf = conf;
    this.rpcAddressList = rpcAddressList;
  }

  public Method buildRpcMethod(String methodName, Class... paramClasses) throws IOException {
    Method method = null;

    try {
      method = CommitLogServerIF.class.getMethod(methodName, paramClasses);
    } catch (Exception e) {
      throw new IOException("Fail to commit due to : " + e);
    }

    return method;
  }

  public void singleOnewayCall(Method rpcMethod, int index, Object... objects) throws IOException {
    Object target = CRPC.getOnewayProxy(CommitLogServerIF.class, CommitLogServerIF.versionID,
        rpcAddressList[index], conf);

    try {
      rpcMethod.invoke(target, objects);
    } catch (Exception e) {
      throw new IOException("Exception has occurred due to : " + e);
    }
  }

  public Object singleCall(Method rpcMethod, int index, Object... objects) throws IOException {
    Object target = CRPC.getProxyWithoutVersionChecking(CommitLogServerIF.class,
        CommitLogServerIF.versionID, rpcAddressList[index], conf);

    try {
      return rpcMethod.invoke(target, objects);
    } catch (InvocationTargetException e) {
      Throwable t = e.getTargetException();

      while(t instanceof InvocationTargetException) {
        t = ((InvocationTargetException) t).getTargetException();
      }
      
      while(t instanceof UndeclaredThrowableException) {
        t = ((UndeclaredThrowableException) t).getUndeclaredThrowable();
      }
      
      throw new IOException("fail to invoke rpc [" + rpcMethod.getName() + "], addr : "
          + rpcAddressList[index] + " due to " + t, t);
    } catch (Exception e) {
      throw new IOException("Exception has occurred due to : " + e);
    }
  }

  public Object[] multiSerialCall(Method method, Object... objects) throws IOException {
    Object[] retList = new Object[rpcAddressList.length];

    for (int i = 0; i < rpcAddressList.length; i++) {
      retList[i] = singleCall(method, i, objects);
    }

    return retList;
  }

//  public Object[] multiParallelCall(Method method, Object... objects) throws IOException {
//    Object[][] params = new Object[rpcAddressList.length][];
//
//    for (int i = 0; i < rpcAddressList.length; i++) {
//      int subIndex = 0;
//      params[i] = new Object[objects.length];
//
//      for (Object param : objects) {
//        params[i][subIndex++] = param;
//      }
//    }
//
//    return NRPC.call(method, params, rpcAddressList, conf);
//  }

  public Object[] multiParallelCall_v2(final Method method, final Object... objects)
      throws IOException {
    Thread[] threadList = new Thread[rpcAddressList.length];
    final Object[] retList = new Object[rpcAddressList.length];

    for (int i = 0; i < rpcAddressList.length; i++) {
      final int index = i;
      threadList[i] = new Thread(new Runnable() {
        public void run() {
          try {
            retList[index] = singleCall(method, index, objects);
          } catch (IOException e) {
            retList[index] = e;
          }
        }
      });
      threadList[i].start();
    }

    for (int i = 0; i < rpcAddressList.length; i++) {
      try {
        threadList[i].join();
      } catch (InterruptedException e) {
      }

      if (retList[i] instanceof IOException) {
        throw (IOException) retList[i];
      }
    }

    return retList;

  }

  public int getMultiRpcCount() {
    return rpcAddressList.length;
  }

  public InetAddress getAddressAt(int index) {
    return rpcAddressList[index].getAddress();
  }
}
