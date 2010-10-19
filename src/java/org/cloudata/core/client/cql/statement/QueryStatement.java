package org.cloudata.core.client.cql.statement;

import org.cloudata.core.client.shell.HelpUsage;
import org.cloudata.core.common.conf.CloudataConf;


public interface QueryStatement {
  public String getQuery(CloudataConf conf);
  public ExecStatus execute(CloudataConf conf);
  public String getPrefix();
  public HelpUsage getHelpUsage();
}
