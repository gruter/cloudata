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

package org.cloudata.core.common.io;

import java.util.HashMap;

import org.cloudata.core.common.conf.CConfigurable;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.util.ReflectionUtils;


/** Factories for non-public writables.  Defining a factory permits {@link
 * CObjectWritable} to be able to construct instances of non-public classes. */
public class CWritableFactories {
  private static final HashMap<Class, CWritableFactory> CLASS_TO_FACTORY =
    new HashMap<Class, CWritableFactory>();

  private CWritableFactories() {}                  // singleton

  /** Define a factory for a class. */
  public static synchronized void setFactory(Class c, CWritableFactory factory) {
    CLASS_TO_FACTORY.put(c, factory);
  }

  /** Define a factory for a class. */
  public static synchronized CWritableFactory getFactory(Class c) {
    return CLASS_TO_FACTORY.get(c);
  }

  /** Create a new instance of a class with a defined factory. */
  public static CWritable newInstance(Class c, CloudataConf conf) {
    CWritableFactory factory = CWritableFactories.getFactory(c);
    if (factory != null) {
      CWritable result = factory.newInstance();
      if (result instanceof CConfigurable) {
        ((CConfigurable) result).setConf(conf);
      }
      return result;
    } else {
      return (CWritable)ReflectionUtils.newInstance(c, conf);
    }
  }
  
  /** Create a new instance of a class with a defined factory. */
  public static CWritable newInstance(Class c) {
    return newInstance(c, null);
  }

}

