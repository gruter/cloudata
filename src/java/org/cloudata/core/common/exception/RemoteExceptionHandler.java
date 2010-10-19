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
package org.cloudata.core.common.exception;

import java.io.IOException;
import java.lang.reflect.Constructor;

import org.cloudata.core.common.ipc.CRemoteException;


public class RemoteExceptionHandler {
  private RemoteExceptionHandler() {}
  
  public static IOException decodeRemoteException(IOException exception) throws IOException {
    if( !(exception instanceof CRemoteException) ) {
      return exception;
    }
    
    IOException ioE = exception;
    CRemoteException re = (CRemoteException)exception;
    try {
      Class c = Class.forName(re.getClassName());

      Class[] parameterTypes = { String.class };
      Constructor ctor = c.getConstructor(parameterTypes);
      
      Object[] arguments = { re.getMessage() };
      Throwable t = (Throwable) ctor.newInstance(arguments);
      
      if (t instanceof IOException) {
        ioE = (IOException) t;

      } else {
        ioE = new IOException("server error");
        ioE.initCause(t);
        throw ioE;
      }
    } catch (Exception e) {
      return new IOException("RPC Exception decode error:" + re.getClassName() + "," + e.getMessage());
    }
    return ioE;
  }
}
