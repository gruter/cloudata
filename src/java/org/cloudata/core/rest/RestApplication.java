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
package org.cloudata.core.rest;

/**
 * @author doe-nam kim
 *
 */
import org.restlet.Application;
import org.restlet.Restlet;
import org.restlet.Router;

/**
 * GET /table: get table list<br>
 * GET /table/{table_name}: get table schema<br>
 * POST /table/{table_name}: create table<br>
 * DELETE /table/{table_name}: drop table<br>
 * PUT /{table_name}: put table data<br>
 * GET /{table_name}/{row_key}: get table data<br>
 * GET /{table_name}/{row_key}?page_size=10: get table data<br>
 * DELETE /{table_name}/{row_key}: delete table data<br>
 * GET /{table_name}/{row_key}/{column_name}: get table data<br>
 * GET /{table_name}/{row_key}/{column_name}/{cell_key}: get table data<br>
 *
 * TableSchema XML:<br>
 * <pre> 
 *   <cloudata>
 *     <table name="T_BLOB_TEST01">
 *       <description></description>
 *       <column name="LOCAL_PATH" type="normal"></column>
 *       <column name="UPLOADED_FILE" type="blob"></column>
 *     </table>
 *   </cloudata>
 * </pre>
 *
 * Row XML: <br>
 * <pre>
 * <cloudata>
 * <row key="RK1">
 *   <column name="COL1">
 *     <cell key="CK1">
 *       <value timestamp="1240377513106">data1</value>
 *     </cell>
 *     <cell key="CK2">
 *       <value timestamp="1240377513106">data2</value>
 *     </cell>
 *   </column>
 *   <column name="COL2">
 *     <cell key="CK1">
 *       <value timestamp="1240377662707">
 *     </cell>
 *   </column>
 * </row>
 * </cloudata>  
 * </pre>
 * 
 */
public class RestApplication extends Application {
	/**
	 * Creates a root Restlet that will receive all incoming calls.
	 */
	@Override
	public synchronized Restlet createRoot() {
		Router router = new Router(getContext());

		router.attach("/table", TableListResource.class);
		router.attach("/table/{table_name}", TableManagerResource.class);
		router.attach("/{table_name}", DataPutResource.class);
		router.attach("/{table_name}/{row_key}", DataRowResource.class);
		router.attach("/{table_name}/{row_key}/{column_name}",
				DataColumnResource.class);
		router.attach("/{table_name}/{row_key}/{column_name}/{cell_key}",
				DataCellResource.class);

		return router;
	}
}