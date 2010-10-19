package org.cloudata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.fs.CloudataFileSystem;
import org.cloudata.core.fs.GPath;

public class Test {
  public static void main(String[] args) throws Exception {
//    Configuration hconf = new Configuration();
//    FileSystem hfs = FileSystem.get(hconf);
//    System.out.println(">>>>" + hfs.mkdirs(new Path(args[0] + "0")));
    
    CloudataConf conf = new CloudataConf();
    CloudataFileSystem fs = CloudataFileSystem.get(conf);
    
    System.out.println(">>>>" + fs.mkdirs(new GPath(args[0] + "1")));
    System.out.println(">>>>" + fs.mkdirs(new GPath(args[0] + "2")));
    System.out.println(">>>>" + fs.mkdirs(new GPath(args[0] + "3")));
    System.out.println(">>>>" + fs.mkdirs(new GPath(args[0] + "4")));
  }
}
