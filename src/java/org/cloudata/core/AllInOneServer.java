package org.cloudata.core;

import org.cloudata.core.common.MiniCloudataCluster;
import org.cloudata.core.common.conf.CloudataConf;

public class AllInOneServer {
  public void startCluster(String dataDir, boolean format) throws Exception {
    CloudataConf conf = new CloudataConf();
    conf.set("cloudata.root", dataDir);
    conf.set("client.tx.timeout", 10);
    conf.set("tabletServer.minMemory", 5);
    new MiniCloudataCluster(conf, 1, 1, format);
    System.out.println("================== Complete starting LocalCluster ==================");
  }
  
  public static void main(String[] args) throws Exception {
    //args = new String[]{"/Users/babokim/tmp/cloudata/data", "Y"};
    if(args.length < 2) {
      System.out.println("Usage: java AllInOneServer <data dir> <format(Y|N)>");
      System.exit(0);
    }
    
    (new AllInOneServer()).startCluster(args[0], "Y".equals(args[1]));
  }
}
