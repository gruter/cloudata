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



public interface Constants {
  //TODO timestamp에 대한 동기화 -> 전체 서버에 대해 동일한 timestamp가 가능하도록
  //TODO Disk로 저장시 오래된 버전의 record는 삭제
  public static final int MAJOR_VERSION = 1;
  public static final int MINOR_VERSION = 0;
  public static final int MINOR_VERSION_2 = 0;
  
  public static final String TABLE_NAME_ROOT = "ROOT";
  public static final String TABLE_NAME_META = "META";
  public static final String TABLE_NAME_BLOB_PREFIX = "_BLOB_";
  
  public static final String META_COLUMN_NAME_TABLETINFO = "TabletInfo";
 
  public static final String BLOB_COLUMN_FILE = "FILE";
  public static final String BLOB_COLUMN_DELETE = "DELETE";
  
  public static final String TABLET_STOP_MARK = "stoped";
  
  public static final String MIN_VALUE = "";
  public static final byte[] MAX_VALUE = new byte[]{
    (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF,
    (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF,
    (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF,
    (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF};
  
  public static final int MAP_RECORD_ROW = 1;
  public static final int MAP_RECORD_COLUMN = 2;
  public static final int INDEX_INTERVAL_LENGH = 100;
  
  public static final int LOG_OP_CREATE_ROW = 1;
  public static final int LOG_OP_ADD_COLUMN_VALUE = 3;
  public static final int LOG_OP_DELETE_COLUMN_VALUE = 4;
  public static final int LOG_OP_MODIFY_META = 5;
  
  public static final int DELETEED = 0;
  public static final int INSERTED = 1;
  
  public static final byte SCANNER_OPEN_SUCCESS = (byte) 80;
  public static final byte SCANNER_OPEN_FAIL = (byte) 81;
  public static final byte SCANNER_END = (byte) 82;
  
  public static final byte UPLOAD_END = (byte) 90;
  public static final byte UPLOAD_START_ROW = (byte) 91;
  public static final byte UPLOAD_FAIL = (byte) 92;
  public static final byte END_RECEIVE_ROW = (byte) 93;
  public static final byte BATCH_RECEIVER_SUCCESS = (byte) 94;
  
  
  public static final byte OPEN_SCANNER = (byte) 60;
  public static final byte BATCH_UPLOAD = (byte) 61;

  //public static final Row.Key MAX_ROW_KEY = new Row.Key(MAX_VALUE);
  //for Unit Test
  public static final boolean TEST_MODE = true;
  
  public static final int MAX_KEY_LENGTH = 1024;
  
  //<ZooKeeper Lock>
  public static final String PATH_SCHEMA = "/lock/schema";
  public static final String SCHEMA_DETAIL = "/_detail";
  public static final String SCHEMA_DESCRIPTION_FILE_NAME = "description_dat";
  public static final String SCHEMA_INFO_FILENAME = "schema.info";
  
  public static final String MASTER = "/lock/master";

  //public static final String MASTER_SERVER = "/lock/master/live_server";

  public static final String ROOT_TABLET = "/lock/rootTablet";
  
  public static final String ROOT_TABLET_HOST = ROOT_TABLET + "/assignedHost";
  
  public static final String SERVER = "/lock/server";

  public static final String COMMITLOG_SERVER = "/commitlog_server";

  public static final String PROCESS = "/lock/process";
  
  public static final String COMPACTION = "/lock/compaction";
  
  public static final String UPLOADER = "/lock/uploader";
  
  public static final String SPLIT = "/lock/split";
  public static final String SPLIT_INFO = SPLIT + "/split.info";
  public static final String SPLIT_STORE = SPLIT + "/split_store";

  public static final String TABLETSERVER_SPLIT = "/lock/tabletserver_split";

  public static final String TABLET_DROP = "/lock/tablet_drop";
  public static final String TABLE_DROP = "/lock/table_drop";

  public static final String USERS = "/lock/users";
  public static final String GROUPS = "/lock/groups";
  public static final String SUPERGROUP = "/lock/groups/supergroup";
  public static final String SC_LOCK_PATH = "/lock/sclock";
  public static final String MC_LOCK_PATH = "/lock/mclock";
  public static final String SC_LOCK_NAME = "split-compaction-lock";
  public static final String MC_LOCK_NAME = "minor-compaction-lock";
  
//  public static final String SCHEMA = "/lock/schema";
//  public static final String SCHEMA_INFO = SCHEMA + "/schema.info";
//  public static final String TABLE_TRUNCATE = SCHEMA + "/truncate";
  
  public static final String TABLET_CREATED = "TabletServerCreated";  
  public static final String TABLET_FAIL = "TabletServerFailed";    
  public static final String TABLET_AVAIL = "TabletServerAvailable";  
  public static final String TABLET_ASSIGN = "TabletServerAssigned";  
  public static final String TABLET_ALIVE = "TabletServerAlive";  
  public static final String TABLE_LOCK = "/lock/table_lock";
  //</ZooKeeper Lock>
  
  public static final String PIPE_CL_FILE_NAME = "PipeBasedCommitLog.";
  public static final int PIPE_DISCONNECT = -0xBABE;
  public static final byte PIPE_DISCONNECT_OK = (byte) 0x6d;
  public static final String PIPE_CONNECTED = "OK";
  public static final String PIPE_IDX_FILE_NAME = "index.";
}
