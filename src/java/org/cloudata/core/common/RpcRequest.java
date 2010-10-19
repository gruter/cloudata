package org.cloudata.core.common;

import java.lang.reflect.Method;
import java.net.InetSocketAddress;

import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.ipc.CRPC;


public class RpcRequest {
	public static void main(String[] args) {
		if (args.length < 6) {
			System.err.println("usage : org.cloudata.core.common.RpcRequest ip port className version methodName arg");
			System.exit(2);
		}

		CloudataConf conf = new CloudataConf();
		String ip = args[0];
		int port = Integer.parseInt(args[1]);
		String className = args[2];
		long version = Long.parseLong(args[3]);
		String methodName = args[4];
		String arg = args[5];

		Class<?> c = null;
		try {
			c = Class.forName(className);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}

		InetSocketAddress addr = new InetSocketAddress(ip, port);

		try {
			Object target = CRPC.getProxy(c, version, addr, conf);
			Method method = c.getDeclaredMethod(methodName, String.class);
			Object ret = method.invoke(target, arg);

			if (ret != null) {
				System.out.println(ret);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}

