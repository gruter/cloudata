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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.restlet.Context;
import org.restlet.data.MediaType;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.data.Status;
import org.restlet.resource.Representation;
import org.restlet.resource.Resource;
import org.restlet.resource.ResourceException;
import org.restlet.resource.StringRepresentation;
import org.restlet.resource.Variant;

/**
 * @author doe-nam kim
 *
 */
public class DataColumnResource extends Resource {
  private static final Log LOG = LogFactory.getLog(DataColumnResource.class.getName());
  
	private CloudataRestService cloudataService = new CloudataRestService();
	private String tableName, rowKey, columnName;
	
	public DataColumnResource(Context context, Request request, Response response) {
		super(context, request, response);
		
		getVariants().add(new Variant(MediaType.TEXT_XML));

		tableName = (String) getRequest().getAttributes().get("table_name");
		rowKey = (String) getRequest().getAttributes().get("row_key");
		columnName = (String) getRequest().getAttributes().get("column_name");
	}

	@Override
	public Representation represent(Variant variant) throws ResourceException {
		if (StringUtils.isEmpty(tableName) || StringUtils.isEmpty(rowKey)
				|| StringUtils.isEmpty(columnName)) {
			getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
			return new StringRepresentation(
					cloudataService.getResponseMessage("Required tableName, rowKey, columnName, cellKey", true),  
					MediaType.TEXT_PLAIN);
		} else {
			try {
				String result = cloudataService.getData(tableName, rowKey,
						columnName);
				getResponse().setStatus(Status.SUCCESS_OK);
				return new StringRepresentation(result, MediaType.TEXT_PLAIN);
			} catch (Exception e) {
	      LOG.error(e);
	      return new StringRepresentation(cloudataService.getErrorMessage(e),
	          MediaType.TEXT_PLAIN); 
			}
		}
	}

}
