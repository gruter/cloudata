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
package org.cloudata.core.tabletserver;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Random;

import org.cloudata.core.common.io.CWritable;
import org.cloudata.core.common.io.CWritableUtils;


public class TxId implements CWritable {
	private String txId;
	private boolean systemTimestamp;
  
	public TxId() {
		
	}
	
  public TxId(String txId) {
    this(txId, true);
  }
  
	public TxId(String txId, boolean systemTimestamp) {
		this.txId = txId;
    this.systemTimestamp = systemTimestamp;
	}
  
	public boolean isSystemTimestamp() {
    return systemTimestamp;
  }

  public void setSystemTimestamp(boolean systemTimestamp) {
    this.systemTimestamp = systemTimestamp;
  }

  public String getTxId() {
		return txId;
	}
 
	public void setTxId(String txId) {
		this.txId = txId;
	}
	
	public void readFields(DataInput in) throws IOException {
		txId = CWritableUtils.readString(in);
    systemTimestamp = "Y".equals(CWritableUtils.readString(in));
	}

	public void write(DataOutput out) throws IOException {
		CWritableUtils.writeString(out, txId);
    CWritableUtils.writeString(out, systemTimestamp ? "Y" : "N");
	}
	
	public String toString() {
		return txId;
	}
	
	public int hashCode() {
		return txId.hashCode();
	}	
	
	@Override
	public boolean equals(Object obj) {
		if( !(obj instanceof TxId) )	return false;
		
		TxId otherTxId = (TxId)obj;
		return txId.equals(otherTxId.txId);
	}

	private static Random rand = new Random();
	
	public static TxId generate(String tabletName, boolean systemTimestamp) {
      TxId txId = new TxId(tabletName + 
          String.valueOf(System.currentTimeMillis()) +
          String.valueOf(System.nanoTime()) +
          String.valueOf(rand.nextInt()), systemTimestamp);   
      return txId;
	}
}
