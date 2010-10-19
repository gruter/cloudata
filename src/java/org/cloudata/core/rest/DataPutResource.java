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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.restlet.Context;
import org.restlet.data.Form;
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
public class DataPutResource extends Resource {
  private static final Log LOG = LogFactory.getLog(DataPutResource.class.getName());
  
	CloudataRestService cloudataService = new CloudataRestService();
	String tableName;
	
	public DataPutResource(Context context, Request request, Response response) {
		super(context, request, response);
		
		getVariants().add(new Variant(MediaType.TEXT_XML));
		this.setModifiable(true);
		
		tableName = (String) getRequest().getAttributes().get("table_name");
	}

	@Override
	public void acceptRepresentation(Representation entity)
			throws ResourceException {
		Form form = new Form(entity);
		String documentXML = form.getFirstValue("data");
		Representation rep = null;

		try {
			cloudataService.putData(tableName, documentXML);
			getResponse().setStatus(Status.SUCCESS_CREATED);
			rep = new StringRepresentation("Success", MediaType.TEXT_XML);
		} catch (Throwable t) {
      LOG.error(t);
      rep = new StringRepresentation(cloudataService.getErrorMessage(t),
          MediaType.TEXT_PLAIN);   
		}

		getResponse().setEntity(rep);
	}
}
