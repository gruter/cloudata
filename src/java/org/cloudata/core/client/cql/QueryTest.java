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
package org.cloudata.core.client.cql;

import org.cloudata.core.client.cql.javacc.CqlParser;
import org.cloudata.core.client.cql.statement.QueryStatement;
import org.cloudata.core.common.conf.CloudataConf;


/**
 * @author jindolk
 *
 */
public class QueryTest {
  public static void main(String args[]) throws Exception {
    testQuery("drop table T_TEST");
    testQuery("create table T_TEST (col1, col2)");
    testQuery("create table T_TEST (col1, user)");
    testQuery("create table T_TEST (col1, col2) VERSION = 1");
    testQuery("create table T_TEST (col1, col2) COMMENT = 'this is test'");
    testQuery("create table T_TEST (col1, col2) VERSION = 1 COMMENT = 'this is test'");
    testQuery("insert into T_TEST values ( ('ck1', 'value1') ) where rowkey = '1'");
    testQuery("insert into T_TEST values ( ('ck1', 'value1'), ('ck2', 'value2')) where rowkey = '1'");
    testQuery("insert into T_TEST (co1, col2) values ( ('ck1', 'value1'), ('ck2', 'value2')) where rowkey = '1'");
    testQuery("select * from T_TEST");
    testQuery("select col1 from T_TEST");
    testQuery("select col1, col2 from T_TEST");
    testQuery("select * from T_TEST where rowkey = '1234'");
    testQuery("select * from T_TEST where rowkey = '1234' and col1 = '1234'");
    testQuery("select * from T_TEST where rowkey like '1234'");
    testQuery("select * from T_TEST where rowkey between '1234' and '3456'");
    testQuery("select * from T_TEST where rowkey = '1234' and col2 = 'b'");
    testQuery("select * from T_TEST where rowkey = '1234' or col2 = 'b' and col3 like 'aaa'");
    testQuery("select * from T_TEST where rowkey = '1234' and col1.timestamp between '20080101' and '20081231'");
    
    testQuery("delete from T_TEST where rowkey = '1234'");
    testQuery("delete col1 from T_TEST where rowkey = '1234'");
    testQuery("delete col1 from T_TEST where rowkey = '1234' and col1 = '123'");
    testQuery("delete col1, col2 from T_TEST where rowkey = '1234'");
    testQuery("delete * from T_TEST where rowkey = '1234'");
}  
  
  private static void testQuery(String query) {
    try {
      CqlParser parser = new CqlParser(query);
      QueryStatement stmt = parser.parseStmt();
      System.out.println(stmt.getQuery(new CloudataConf()));
    } catch (Exception e) {
      e.printStackTrace(System.out);
    }
  }
}
