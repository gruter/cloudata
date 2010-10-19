package org.cloudata.core;

import org.cloudata.core.client.TestCTable;
import org.cloudata.core.commitlog.TestCommitLogClient;
import org.cloudata.core.commitlog.TestCommitLogFileSystem;
import org.cloudata.core.commitlog.TestCommitLogServerFailure;
import org.cloudata.core.commitlog.TestServerLocationManager;
import org.cloudata.core.commitlog.pipe.TestBufferPool;
import org.cloudata.core.common.ipc.TestRPCTimeOutHandling;
import org.cloudata.core.common.testhelper.TestFaultInjectionProxy;
import org.cloudata.core.common.testhelper.TestFaultManager;
import org.cloudata.core.fs.TestPipeBasedCommitLogFileSystem;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;


public class AllSuite extends TestCase {
  public static Test suite() throws Exception {
    TestSuite suite = new TestSuite();
    suite.addTestSuite(TestFaultInjectionProxy.class);
    suite.addTestSuite(TestFaultManager.class);

    suite.addTestSuite(TestBufferPool.class);
    
    suite.addTestSuite(TestRPCTimeOutHandling.class);
    
    suite.addTestSuite(TestCommitLogClient.class);
    suite.addTestSuite(TestCommitLogFileSystem.class);
    suite.addTestSuite(TestCommitLogServerFailure.class);
    suite.addTestSuite(TestServerLocationManager.class);
    
    suite.addTestSuite(TestPipeBasedCommitLogFileSystem.class);
    
    //suite.addTestSuite(TestNTable.class);
    
    return suite;
  }
}
