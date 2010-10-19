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
package org.cloudata.core.common.aop;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.Iterator;
import java.util.List;

import org.cloudata.core.tabletserver.TabletServer;


public class ProxyFactory {
  public static Object createProxyClass(String classId) throws IOException {
    try {
      ClassInfo classInfo = ClassInfoUtil.getInstance().getClassInfo(classId);
      Object targetObject = Class.forName(classInfo.getClassName()).newInstance();
      if(!(targetObject instanceof ProxyObject)) {
        throw new IOException(classId + " is not proxy object");
      }

      List<String> handlers = classInfo.getHandler();

      if (handlers == null || handlers.size() == 0) {
        return targetObject;
      } else {
        ProxyObject handlerTargetObject = (ProxyObject)targetObject;
        ProxyObject proxy = null;
        for (Iterator it = handlers.iterator(); it.hasNext();) {
          String handlerType = (String) it.next();
          DefaultInvocationHandler handler = (DefaultInvocationHandler) Class.forName(handlerType).newInstance();
//          if(targetObject instanceof TabletServer) {
//            System.out.println("createProxyClass>>>>>>Handler>" + handler.getClass() + ">" + ((TabletServer)handlerTargetObject).getHostName());
//          }
          handler.setTargetObject(handlerTargetObject);
          Object tmpObj = Proxy.newProxyInstance(
              targetObject.getClass().getClassLoader(), 
              targetObject.getClass().getInterfaces(), 
              handler);
          
          proxy = (ProxyObject)tmpObj;
          
          handlerTargetObject = proxy;
        }
        return proxy;
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new IOException(e.getMessage());
    }
  }
}
