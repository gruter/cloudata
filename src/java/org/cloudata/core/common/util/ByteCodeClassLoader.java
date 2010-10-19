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

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.common.exception.GetFailException;


/**
 * @author jindolk
 *
 */
public class ByteCodeClassLoader extends ClassLoader {
  static final Log LOG = LogFactory.getLog(ByteCodeClassLoader.class.getName());

  private final Map<String, VersionedClass> classes = new HashMap<String, VersionedClass>();
  private ClassLoader parent;
  
  public ByteCodeClassLoader(ClassLoader parent) {
    super(parent);
    this.parent = parent;
  }

  public byte[] getClassBytesFromCache(Class<?> clazz, int version) {
    synchronized (classes) {
      if (classes.containsKey(clazz.getName())) {
        VersionedClass vClass = classes.get(clazz.getName());
        if (vClass.version == version) {
          return vClass.classBytes;
        } else {
          classes.remove(clazz.getName());
          return null;
        }
      }
      return null;
    }
  }

  public void addClassBytes(String className, int version, byte[] classBytes) {
    synchronized (classes) {
      if (classes.containsKey(className)) {
        VersionedClass vClass = classes.get(className);
        if (vClass.version == version) {
          return;
        } else {
          classes.put(className, new VersionedClass(version, classBytes));
        }
      } else {
        classes.put(className, new VersionedClass(version, classBytes));
      }
    }
  }

  @Override
  protected synchronized Class<?> loadClass(String name, boolean resolve)
      throws ClassNotFoundException {
    Class<?> clazz = findClass(name);
    if(clazz == null) {
      return parent.loadClass(name);
    } else {
      return clazz;
    }
  }

  @Override
  public Class<?> findClass(String name) throws ClassNotFoundException {
    try {
      byte[] byteClass = null;
      synchronized (classes) {
        VersionedClass vClass = classes.get(name);
        if(vClass != null) {
          byteClass = vClass.classBytes;
        } else {
        }
      }
      if (byteClass == null) {
        return null;
      }
      Class<?> clazz = defineClass(name, byteClass, 0, byteClass.length);
      return clazz;
    } catch (Error e) {
      LOG.error("Class load error:" + e.getMessage(), e);
      return super.findSystemClass(name);
    }
  }

  public byte[] getClassBytes(Class<?> clazz, int version)
      throws IOException {
    byte[] classBytes = getClassBytesFromCache(clazz, version);
    if (classBytes != null) {
      return classBytes;
    } else {
      classBytes = loadClassBytes(clazz);
      addClassBytes(clazz.getName(), version, classBytes);
      return classBytes;
    }
  }

  private byte[] loadClassBytes(Class<?> clazz) throws IOException {
    InputStream in = ByteCodeClassLoader.class.getClassLoader().getResourceAsStream(
        clazz.getName().replaceAll("\\.", "/") + ".class");
    if (in == null) {
      throw new GetFailException("No class data:" + clazz);
    }

    BufferedInputStream bin = new BufferedInputStream(in);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
      byte[] buf = new byte[4096];
      int read = 0;
      while ((read = bin.read(buf)) > 0) {
        out.write(buf, 0, read);
      }
    } finally {
      bin.close();
      out.close();
    }
    return out.toByteArray();
  }  
}

class VersionedClass {
  int version;

  byte[] classBytes;

  public VersionedClass(int version, byte[] classBytes) {
    this.version = version;
    this.classBytes = classBytes;
  }
}