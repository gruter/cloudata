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
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.common.conf.CloudataConf;


public class FaultInjectionProxy {
  static final Log LOG = LogFactory.getLog(FaultInjectionProxy.class);
  static Object fmLock = new Object();
  static FaultManager fm = null;

  // by sangchul 경고를 낼 필요가 없다. 항상 Type safe 하다.
  @SuppressWarnings("unchecked")
  public static <T> T wrap(Object body, Class<? extends T> c) {
    synchronized (fmLock) {
      if (fm == null) {
        try {
          fm = FaultManager.create(new CloudataConf());
        } catch (IOException e) {
          LOG.warn("Fail to create fault manager", e);
        }
      }
    }
    return (T) Proxy.newProxyInstance(c.getClassLoader(), 
        new Class<?>[] { c },
        new FaultInjectionHandler(body));
  }

  static class FaultInjectionHandler implements InvocationHandler {
    Object body;

    public FaultInjectionHandler(Object body) {
      this.body = body;
    }

    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      Object ret = null;
      Fault fault = null;

      synchronized (fmLock) {
        fault = fm.getFault(proxy.getClass(), method.getName());
      }

      if (fault != null) {
        fault.playBeforeInvoke(body);
        try {
          ret = method.invoke(body, args);
        } finally {
          fault.playAfterInvoke(body);
        }
      } else {
        ret = method.invoke(body, args);
      }

      return ret;
    }
  }
}
