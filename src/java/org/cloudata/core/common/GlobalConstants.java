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
package org.cloudata.core.common;

import org.cloudata.core.tablet.TableSchema;
import org.cloudata.core.tablet.TableSchema.ColumnInfo;

public class GlobalConstants implements Constants {
  public static TableSchema ROOT_TABLE = null;
  public static TableSchema META_TABLE = null;
  
  static {
    ROOT_TABLE = new TableSchema(TABLE_NAME_ROOT, "root");
    ROOT_TABLE.addColumn(new ColumnInfo(META_COLUMN_NAME_TABLETINFO, TableSchema.CACHE_TYPE));
    ROOT_TABLE.setOwner(System.getProperty("user.name"));
    ROOT_TABLE.addPermission("*", "r");
    
    META_TABLE = new TableSchema(TABLE_NAME_META, "meta");
    META_TABLE.addColumn(new ColumnInfo(META_COLUMN_NAME_TABLETINFO, TableSchema.CACHE_TYPE));
    META_TABLE.setOwner(System.getProperty("user.name"));
    META_TABLE.addPermission("*", "r");
  }
}
