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
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.lock.LockUtil;
import org.cloudata.core.common.testhelper.FaultManager.FaultHelper.InjectionInfo;


public class FaultManager {
  private static final Log LOG = LogFactory.getLog(FaultManager.class);
  private final static String LOCK_PATH = "/fault_injection";

  private HashMap<Class<?>, HashMap<String, Fault>> classFaultMap 
      = new HashMap<Class<?>, HashMap<String, Fault>>();
  ZooKeeper zk;
  CloudataConf conf;
  
  FaultManager(CloudataConf conf) throws IOException {
    this.conf = conf;
    initLockClient(conf);
  }

  public static FaultManager create(CloudataConf conf) throws IOException {
    FaultManager fm = new FaultManager(conf);
    fm.load();
    return fm;
  }

  public void close() {
  }

  public Fault getFault(Class<?> c, String methodName) throws IOException {
    if (zk == null) {
      throw new IOException("FaultManager is not initialized");
    }

    HashMap<String, Fault> faultMap = null;
    synchronized (classFaultMap) {
      faultMap = findFaultMap(c);
    }

    if (faultMap == null) {
      return null;
    }

    return faultMap.get(methodName);
  }

  public void clearAllFaults() throws IOException, InterruptedException {
    LockUtil.delete(zk, LockUtil.getZKPath(conf, LOCK_PATH), true);
    
    Condition cond = new Condition() {
      boolean isSatisfied() {
        return classFaultMap.isEmpty();
      }
    };

    waitUntil(cond, 10000);
  }

  public void clear(final Class<?> c, final String methodName) throws IOException, InterruptedException {
    if (zk == null) {
      throw new IOException("FaultManager is not initialized");
    }

    String faultLockPath = FaultHelper.createLockPath(c, methodName);
    try {
      zk.setData(LockUtil.getZKPath(conf, LOCK_PATH + "/" + faultLockPath), "".getBytes(), -1);
    } catch (Exception e) {
      throw new IOException(e);
    }

    Condition cond = new Condition() {
      boolean isSatisfied() throws IOException {
        return getFault(c, methodName) == null;
      }
    };
    
    waitUntil(cond, 5000);
  }

  /**
   * Pleiades에 fault 정보를 주입한다. 
   * 이 함수의 수행은 fault를 주입한 FaultManager에 해당 fault가 이벤트로써 수신될 때까지
   * blocking한다. 하지만, 이 blocking이 풀린다고 해서 이 fault를 수신해야 하는 
   * 모든 FaultManager가 방금 주입한 fault를 성공적으로 수신한 것은 아니다. 
   * 따라서, 모든 FaultManager가 fault를 수신할 때까지 약간의 sleep을 한다. 
   * @param c
   * @param methodName
   * @param faultClass
   * @throws IOException
   * @throws InterruptedException 
   */
  public void inject(final Class<?> c, final String methodName, final Class<? extends Fault> faultClass) throws IOException, InterruptedException {
    if (zk == null) {
      throw new IOException("FaultManager is not initialized");
    }

    String faultLockPath = FaultHelper.createLockPath(c, methodName);

    try {
      zk.setData(LockUtil.getZKPath(conf, LOCK_PATH + "/" + faultLockPath)
          , faultClass.getName().replaceAll("\\.", "_").getBytes(), -1);
    } catch (Exception e) {
      throw new IOException(e);
    }

    Condition cond = new Condition() {
      boolean isSatisfied() throws IOException {
        Fault fault = null;
        return (fault = getFault(c, methodName)) != null && fault.getClass().equals(faultClass);
      }
    };
    
    waitUntil(cond, 5000);
  }

  private void load() throws IOException {
    List<String> dirs = null;
    try {
      dirs = zk.getChildren(LockUtil.getZKPath(conf, LOCK_PATH), false);
    } catch(NoNodeException e) {
    } catch (Exception e) {
      throw new IOException(e);
    }

    if (dirs == null || dirs.size() == 0) {
      return;
    }

    for (String eachPath : dirs) {
      String fullPath = LOCK_PATH + "/" + eachPath;
      String classAndMethodName = extractClassAndMethodName(fullPath);
      String[] pathElements = classAndMethodName.split("/");

      if (pathElements.length != 2) {
        continue;
      }

      InjectionInfo info;
      try {
        info = FaultHelper.convertAsInfo(fullPath, new String(zk.getData(LockUtil.getZKPath(conf, fullPath), false, null)));
      } catch (Exception e) {
        throw new IOException(e);
      }

      if (info.fault != null) {
        LOG.debug("load fault : " + info);
        internalInject(info.c, info.methodName, info.fault);
      }
    }
  }

  private String extractClassAndMethodName(String fullPath) {
    return fullPath.substring(fullPath.indexOf(LOCK_PATH) + LOCK_PATH.length() + 1);
  }

  private void initLockClient(CloudataConf conf) throws IOException {
    zk = LockUtil.getZooKeeper(conf, "FaultManager", null);
    try {
      if(zk.exists(LockUtil.getZKPath(conf, LOCK_PATH), false) == null) {
        LockUtil.createNodes(zk, LockUtil.getZKPath(conf, LOCK_PATH), null, CreateMode.PERSISTENT);
      }

      List<String> paths =  zk.getChildren(LockUtil.getZKPath(conf, LOCK_PATH), new LockWatcher());
      for(String eachPath: paths) {
        zk.exists(LockUtil.getZKPath(conf, LOCK_PATH + "/" + eachPath), new LockWatcher());
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  class LockWatcher implements Watcher {
    @Override
    public void process(WatchedEvent event) {
      if(event.getType() == Event.EventType.NodeChildrenChanged || event.getType() == Event.EventType.NodeDataChanged) {
        String path = event.getPath();
        if (path.equals(LOCK_PATH)) {
          // clear all
          synchronized(classFaultMap) {
            classFaultMap.clear();
          }
        } else {
          InjectionInfo info = null;
          try {
            info = FaultHelper.convertAsInfo(path, 
                new String(zk.getData(path, false, null)));
          } catch (NoNodeException e) {
          } catch (Exception e) {
            LOG.error(e);
            return;
          }
    
          if (info.fault == null) {
            LOG.debug("clear fault event [" + path + "]");
            internalClear(info.c, info.methodName);
          } else {
            LOG.debug("inject fault event [" + path + "], fault class :"
                + info.fault.getClass().getName());
            internalInject(info.c, info.methodName, info.fault);
          }
        }
      }
    }
  }
    
//      public LockEventFilter getFilter() {
//        LockEventFilter filter = new LockEventFilter();
//        filter.addConstraint(LockEventFilter.Operator.PF, LOCK_PATH);
//
//        List<LockService.Events> lockEvents = new ArrayList<LockService.Events>();
//        lockEvents.add(LockService.Events.NODE_REMOVED);
//        lockEvents.add(LockService.Events.CONTENTS_CHANGED);
//        filter.setLockEventList(lockEvents);
//
//        return filter;
//      }
//      
//      public void notifyEvent(LockService.Events event, LockEventData eventData) {
//        try {
//          String path = eventData.getLockId();
//          if (path.equals(LOCK_PATH)) {
//            // clear all
//            synchronized(classFaultMap) {
//              classFaultMap.clear();
//            }
//          } else {
//            InjectionInfo info = FaultHelper.convertAsInfo(path, lockService.getContents(path));
//
//            if (info.fault == null) {
//              LOG.debug("clear fault event [" + path + "]");
//              internalClear(info.c, info.methodName);
//            } else {
//              LOG.debug("inject fault event [" + path + "], fault class :"
//                  + info.fault.getClass().getName());
//              internalInject(info.c, info.methodName, info.fault);
//            }
//          }
//        } catch (IOException e) {
//          LOG.warn("Fail to process event", e);
//        }
//      }
//    };
//  }

  private void internalClear(Class<?> c, String methodName) {
    synchronized (classFaultMap) {
      HashMap<String, Fault> faultMap = findFaultMap(c);

      if (faultMap != null) {
        faultMap.remove(methodName);
        
        if (faultMap.isEmpty()) {
          classFaultMap.remove(c);
        }
      }

      classFaultMap.notifyAll();
    }
  }

  private void internalInject(Class<?> c, String methodName, Fault fault) {
    synchronized (classFaultMap) {
      HashMap<String, Fault> faultMap = classFaultMap.get(c);
      if (faultMap == null) {
        faultMap = new HashMap<String, Fault>();
        classFaultMap.put(c, faultMap);
      }
      faultMap.put(methodName, fault);
      classFaultMap.notifyAll();
    }
  }

  private HashMap<String, Fault> findFaultMap(Class<?> c) {
    HashMap<String, Fault> ret = classFaultMap.get(c);

    if (ret == null) {
      Class<?>[] interfaces = c.getInterfaces();
      for (Class<?> ci : interfaces) {
        if ((ret = findFaultMap(ci)) != null) {
          return ret;
        }
      }

      return null;
    }

    return ret;
  }
  
  void waitUntil(Condition cond, long waitTime) throws IOException, InterruptedException {
    synchronized (classFaultMap) {
      long begin = System.currentTimeMillis();

      while (!cond.isSatisfied()) {
        classFaultMap.wait(waitTime);
        waitTime -= (System.currentTimeMillis() - begin);
        if (!cond.isSatisfied() && waitTime <= 0) {
          throw new IOException("fail to operation until timeout");
        }
      }
    }
  }
  
  static abstract class Condition {
    abstract boolean isSatisfied() throws IOException;
  }

  static class FaultHelper {
    static final String DELIMINATOR = "||";

    static class InjectionInfo {
      Class<?> c;
      String methodName;
      Fault fault;

      public String toString() {
        return "fault info, class : " + c.getName() + ", method : " + methodName + ", fault : "
            + ((fault == null) ? null : fault.getClass().getName());
      }
    }

    public static InjectionInfo convertAsInfo(String path, String contents) throws IOException {
      InjectionInfo info = new InjectionInfo();
      String[] pathElements = path.split("/");
      String className = null;

      try {
        String unConvertedClassName = pathElements[pathElements.length - 2];
        className = returnToClassName(unConvertedClassName);
        info.c = Class.forName(className);
      } catch (ClassNotFoundException e) {
        throw new IOException("class name is invalid [" + className + "]");
      }

      info.methodName = pathElements[pathElements.length - 1];

      if (contents.equals("")) {
        info.fault = null;
      } else {
        String faultName = null;
        try {
          faultName = returnToClassName(contents);
          info.fault = (Fault) Class.forName(faultName).newInstance();
        } catch (ClassNotFoundException e) {
          throw new IOException("class name is invalid [" + faultName + "]");
        } catch (Exception e) {
          throw new IOException("fail to instantiate fault", e);
        }
      }

      return info;
    }

    public static String convertAsContent(Class<?> c, String methodName, Fault fault) {
      return c.getName() + DELIMINATOR + methodName + DELIMINATOR + fault.getClass().getName();
    }

    public static String createLockPath(Class<?> c, String methodName) {
      return c.getName().replaceAll("\\.", "_") + "/" + methodName;
    }

    public static String returnToClassName(String convertedClassName) {
      return convertedClassName.replaceAll("_", "\\.");
    }
  }
}
