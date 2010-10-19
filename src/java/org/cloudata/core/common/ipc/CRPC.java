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

package org.cloudata.core.common.ipc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.net.SocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.ZooKeeper;
import org.cloudata.core.common.conf.CConfigurable;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.exception.RemoteExceptionHandler;
import org.cloudata.core.common.io.CObjectWritable;
import org.cloudata.core.common.io.CUTF8;
import org.cloudata.core.common.io.CWritable;
import org.cloudata.core.common.metrics.RPCServerMetrics;
import org.cloudata.core.common.util.NetUtils;


/**
 * A simple RPC mechanism. A <i>protocol</i> is a Java interface. All parameters and return types must be one of:
 * <ul>
 * <li>a primitive type, <code>boolean</code>, <code>byte</code>, <code>char</code>, <code>short</code>, <code>int</code>, <code>long</code>, <code>float</code>,
 * <code>double</code>, or <code>void</code>; or</li>
 * <li>a {@link String}; or</li>
 * <li>a {@link CWritable}; or</li>
 * <li>an array of the above types</li>
 * </ul>
 * All methods in the protocol should throw only IOException. No field data of the protocol instance is transmitted.
 */
public class CRPC {
  private static final Log LOG =
    LogFactory.getLog(CRPC.class.getName());

  private CRPC() {}                                  // no public ctor


  /** A method invocation, including the method name and its parameters.*/
  private static class Invocation implements CWritable, CConfigurable {
    private String userId;
    private String methodName;
    private Class[] parameterClasses;
    private Object[] parameters;
    private CloudataConf conf;

    public Invocation() {}

    public Invocation(Method method, Object[] parameters) {
      this(null, method, parameters);
    }

    public Invocation(String userId, Method method, Object[] parameters) {
      if(userId == null) {
        userId = " ";
      }
      this.userId = userId;
      this.methodName = method.getName();
      this.parameterClasses = method.getParameterTypes();
      this.parameters = parameters;
    }

    /** The name of the method invoked. */
    public String getMethodName() { return methodName; }

    /** The parameter classes. */
    public Class[] getParameterClasses() { return parameterClasses; }

    /** The parameter instances. */
    public Object[] getParameters() { return parameters; }

    public void readFields(DataInput in) throws IOException {
      userId = CUTF8.readString(in);
      methodName = CUTF8.readString(in);
      parameters = new Object[in.readInt()];
      parameterClasses = new Class[parameters.length];
      CObjectWritable objectWritable = new CObjectWritable();
      for (int i = 0; i < parameters.length; i++) {
        parameters[i] = CObjectWritable.readObject(in, objectWritable, this.conf);
        parameterClasses[i] = objectWritable.getDeclaredClass();
      }
    }

    public void write(DataOutput out) throws IOException {
      CUTF8.writeString(out, userId);
      CUTF8.writeString(out, methodName);
      out.writeInt(parameterClasses.length);
      for (int i = 0; i < parameterClasses.length; i++) {
        CObjectWritable.writeObject(out, parameters[i], parameterClasses[i],
                                   conf);
      }
    }

    public String toString() {
      StringBuffer buffer = new StringBuffer();
      buffer.append(methodName);
      buffer.append("(");
      for (int i = 0; i < parameters.length; i++) {
        if (i != 0)
          buffer.append(", ");
        buffer.append(parameters[i]);
      }
      buffer.append(")");
      return buffer.toString();
    }

    public void setConf(CloudataConf conf) {
      this.conf = conf;
    }

    public CloudataConf getConf() {
      return this.conf;
    }

  }

  private static Map<CloudataConf, CClient> CLIENTS =
      new HashMap<CloudataConf, CClient>();

  private static synchronized CClient getClient(CloudataConf conf,
      SocketFactory factory) {
    // Construct & cache client.  The configuration is only used for timeout,
    // and Clients have connection pools.  So we can either (a) lose some
    // connection pooling and leak sockets, or (b) use the same timeout for all
    // configurations.  Since the IPC is usually intended globally, not
    // per-job, we choose (a).
    CClient client = CLIENTS.get(conf);
    if (client == null) {
      client = new CClient(CObjectWritable.class, conf, factory);
      CLIENTS.put(conf, client);
    }
    return client;
  }
  
  /**
   * Stop all RPC client connections
   */
  public static synchronized void stopClient(){
    for (CClient client : CLIENTS.values())
      client.stop();
    CLIENTS.clear();
  }

  private static class Invoker implements InvocationHandler {
    private InetSocketAddress address;
    private CClient client;
    private String userId;
    
    public Invoker(InetSocketAddress address, CloudataConf conf,
        SocketFactory factory) {

      this.address = address;
      this.client = getClient(conf, factory);
      this.userId= conf.getUserId();
    }

    public Object invoke(Object proxy, Method method, Object[] args)
      throws Throwable {
      CObjectWritable value = null;
      try {
        value = (CObjectWritable)client.call(new Invocation(userId, method, args), address);
      } catch (SocketTimeoutException e) {
        LOG.warn("SocketTimeoutException: calling " + method.getName() + "," + e.getMessage());
        throw e;
      } catch (InterruptedException e) {
        throw new RPCInterruptedException();
      } catch (Exception e) {
        if(e instanceof CRemoteException) {
          IOException err = RemoteExceptionHandler.decodeRemoteException((CRemoteException)e);
          throw err;
        }
        throw e;
      }
      return value.get();
    }
  }
  
  private static class OnewayInvoker implements InvocationHandler {
    private InetSocketAddress address;
    private CClient client;

    public OnewayInvoker(InetSocketAddress address, CloudataConf conf,
      SocketFactory factory) {

      this.address = address;
      this.client = getClient(conf, factory);
    }

    public Object invoke(Object proxy, Method method, Object[] args)
      throws Throwable {
      CObjectWritable value = null;
      try {
        client.onewayCall(new Invocation(method, args), address);
      } catch (SocketTimeoutException e) {
        LOG.error(method.getName() + "," + e.getMessage());
        throw e;
      }
      return null;
    }
  }
  /**
   * A version mismatch for the RPC protocol.
   */
  public static class VersionMismatch extends IOException {
    private String interfaceName;
    private long clientVersion;
    private long serverVersion;
    
    /**
     * Create a version mismatch exception
     * @param interfaceName the name of the protocol mismatch
     * @param clientVersion the client's version of the protocol
     * @param serverVersion the server's version of the protocol
     */
    public VersionMismatch(String interfaceName, long clientVersion,
                           long serverVersion) {
      super("Protocol " + interfaceName + " version mismatch. (client = " +
            clientVersion + ", server = " + serverVersion + ")");
      this.interfaceName = interfaceName;
      this.clientVersion = clientVersion;
      this.serverVersion = serverVersion;
    }
    
    /**
     * Get the interface name
     * @return the java class name 
     *          (eg. org.apache.hadoop.mapred.InterTrackerProtocol)
     */
    public String getInterfaceName() {
      return interfaceName;
    }
    
    /**
     * Get the client's prefered version
     */
    public long getClientVersion() {
      return clientVersion;
    }
    
    /**
     * Get the server's agreed to version.
     */
    public long getServerVersion() {
      return serverVersion;
    }
  }
  
  public static CVersionedProtocol waitForProxy(Class protocol,
                                               long clientVersion,
                                               InetSocketAddress addr,
                                               CloudataConf conf
                                               ) throws IOException {
    while (true) {
      try {
        return getProxy(protocol, clientVersion, addr, conf);
      } catch(ConnectException se) {  // namenode has not been started
        LOG.info("Server at " + addr + " not available yet, Zzzzz...");
      } catch(SocketTimeoutException te) {  // namenode is busy
        LOG.info("Problem connecting to server: " + addr);
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ie) {
        // IGNORE
      }
    }
  }


  static Map<InetSocketAddress, Long> versionCheckMap = new HashMap<InetSocketAddress, Long>();

  /** Construct a client-side proxy object that implements the named protocol,
   * talking to a server at the named address. */
  public static CVersionedProtocol getProxy(Class<?> protocol,
      long clientVersion, InetSocketAddress addr, CloudataConf conf,
      SocketFactory factory) throws IOException {

    CVersionedProtocol proxy =
        (CVersionedProtocol) Proxy.newProxyInstance(
            protocol.getClassLoader(), new Class[] { protocol },
            new Invoker(addr, conf, factory));
    
    Long serverVersion = null;
    try {
      synchronized(versionCheckMap) {
        if ((serverVersion = versionCheckMap.get(addr)) == null) {
          serverVersion = proxy.getProtocolVersion(protocol.getName(), 
                                                    clientVersion);
          versionCheckMap.put(addr, serverVersion);
        }
      }
    } catch (IOException e) {
      LOG.warn("Error proxy.getProtocolVersion:" + addr + "," + e.getMessage());
      throw e;
    } catch (Exception e) {
      IOException err = new IOException(e.getMessage());
      err.initCause(e);
      throw err;
    }
    
    if (serverVersion == clientVersion) {
      return proxy;
    } else {
      throw new VersionMismatch(protocol.getName(), clientVersion, 
                                serverVersion);
    }
  }
  
  public static CVersionedProtocol getProxyWithoutVersionChecking(Class<?> protocol,
	      long clientVersion, InetSocketAddress addr, CloudataConf conf)
	      throws IOException {
    return (CVersionedProtocol) Proxy.newProxyInstance(
        protocol.getClassLoader(), new Class[] { protocol },
        new Invoker(addr, conf, NetUtils.getDefaultSocketFactory(conf)));
  }
  
  public static Object getOnewayProxy(Class<?> protocol,
    long clientVersion, InetSocketAddress addr,
      CloudataConf conf) throws IOException {
	  
    SocketFactory factory = NetUtils.getDefaultSocketFactory(conf);
    getProxy(protocol, clientVersion, addr, conf, factory);
    CVersionedProtocol proxy = (CVersionedProtocol) Proxy.newProxyInstance(
      protocol.getClassLoader(), new Class[] { protocol }, new OnewayInvoker(addr, conf, factory)
    );
    return proxy;
  }

  /**
   * Construct a client-side proxy object with the default SocketFactory
   * 
   * @param protocol
   * @param clientVersion
   * @param addr
   * @param conf
   * @return a proxy instance
   * @throws IOException
   */
  public static CVersionedProtocol getProxy(Class<?> protocol,
      long clientVersion, InetSocketAddress addr, CloudataConf conf)
      throws IOException {

    return getProxy(protocol, clientVersion, addr, conf, NetUtils
        .getDefaultSocketFactory(conf));
  }

//  public static Server getServer(final Object instance, final String bindAddress, final int port,
//     final int numHandlers,
//     final boolean verbose, NConfiguration conf) throws IOException {
//    return getServer(null,
//        instance, bindAddress, port, numHandlers, verbose, conf);
//  }

  public static Server getServer(ZooKeeper zk,
      final Object instance, final String bindAddress, final int port,
     final int numHandlers,
     final boolean verbose, 
     CloudataConf conf) throws IOException {
    return new Server(zk, instance, conf, bindAddress, port, numHandlers, verbose, null);
  }
  
  /** Construct a server for a protocol implementation instance listening on a
   * port and address. */
  public static Server getServer(ZooKeeper zk,
                                  final Object instance, final String bindAddress, final int port,
                                 final int numHandlers,
                                 final boolean verbose, 
                                 CloudataConf conf,
                                 RPCServerMetrics serverMetrics) 
    throws IOException {
    return new Server(zk, instance, conf, bindAddress, port, numHandlers, verbose, serverMetrics);
  }

  /** An RPC Server. */
  public static class Server extends org.cloudata.core.common.ipc.CServer {
    private Object instance;
    private Class<?> implementation;
    private boolean verbose;
    
    //userId
    private Set<String> users = new HashSet<String>();
    
    private ZooKeeper zk;
    
    private CloudataConf conf;
    
    /** Construct an RPC server.
     * @param instance the instance whose methods will be called
     * @param conf the configuration to use
     * @param bindAddress the address to bind on to listen for connection
     * @param port the port to listen for connections on
     * @param numHandlers the number of method handler threads to run
     * @param verbose whether each call should be logged
     */
    public Server(ZooKeeper zk, 
                  Object instance, CloudataConf conf, String bindAddress,  int port,
                  int numHandlers, boolean verbose, RPCServerMetrics serverMetrics) throws IOException {
      super(bindAddress, port, Invocation.class, numHandlers, conf, serverMetrics);
      this.instance = instance;
      this.implementation = instance.getClass();
      this.verbose = verbose;
      
      this.zk = zk;
      this.conf = conf;
      
      this.users = AclManager.loadUserInfo(conf, zk);
    }

    public CWritable call(CWritable param) throws IOException {
      try {
        Invocation call = (Invocation)param;
        if (verbose) log("Call: " + call);
   
        //LOG.fatal("Call:" + call.getMethodName());
        if(users != null && "!getProtocolVersion".equals(call.getMethodName())) {
          try {
            AclManager.checkUser(conf, zk, users, call.userId);
          } catch (IOException e) {
            LOG.error("checkUser error:" + call.getMethodName());
            throw e;
          }
        }
        
        //TODO ThreadLocal에 대한 lock 처리는 어떻게 해야 하나?
        AclManager.current.set(call.userId);
        
        Method method =
          implementation.getMethod(call.getMethodName(),
                                   call.getParameterClasses());

        Object value = method.invoke(instance, call.getParameters());
        
//        LOG.debug("Served: " + call.getMethodName() + " " + callTime);
        if (verbose) log("Return: "+value);

        return new CObjectWritable(method.getReturnType(), value);

      } catch (InvocationTargetException e) {
        Throwable target = e.getTargetException();
        if (target instanceof IOException) {
          throw (IOException)target;
        } else {
          IOException ioe = new IOException(target.toString());
          ioe.setStackTrace(target.getStackTrace());
          throw ioe;
        }
      } catch (Throwable e) {
        IOException ioe = new IOException(e.toString());
        ioe.setStackTrace(e.getStackTrace());
        throw ioe;
      }
    }
  }

  private static void log(String value) {
    if (value!= null && value.length() > 55)
      value = value.substring(0, 55)+"...";
    LOG.info(value);
  }
}
