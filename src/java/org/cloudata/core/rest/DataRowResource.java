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
import org.restlet.resource.Representation;
import org.restlet.resource.Resource;
import org.restlet.resource.ResourceException;
import org.restlet.resource.StringRepresentation;
import org.restlet.resource.Variant;

/**
 * @author doe-nam kim
 *
 */
public class DataRowResource extends Resource {
  private static final Log LOG = LogFactory.getLog(DataRowResource.class.getName());
  
	CloudataRestService cloudataService = new CloudataRestService();
	String tableName, rowKey;

	public DataRowResource(Context context, Request request, Response response) {
		super(context, request, response);
		this.setModifiable(true); // Method allow
		
		getVariants().add(new Variant(MediaType.TEXT_XML));

		tableName = (String) getRequest().getAttributes().get("table_name");
		rowKey = (String) getRequest().getAttributes().get("row_key");
	}

	@Override
	public Representation represent(Variant variant) throws ResourceException {
    StringRepresentation rep;
    
    Form form = getRequest().getResourceRef().getQueryAsForm();
    String pageSizeStr = form.getFirstValue("page_size");
    
    try {
      String xml;
      
      if(pageSizeStr == null || pageSizeStr.length() == 0) {
        xml = cloudataService.getData(tableName, rowKey);
      } else {
        xml = cloudataService.getData(tableName, rowKey, Integer.parseInt(pageSizeStr));
      }
      rep = new StringRepresentation(xml);
    } catch (Throwable t) {
      LOG.error(t);
      rep = new StringRepresentation(cloudataService.getErrorMessage(t),
          MediaType.TEXT_PLAIN);   
    }
    
    return rep;
	}

	@Override
	public void removeRepresentations() {
		StringRepresentation rep = null;

		try {
			cloudataService.removeData(tableName, rowKey);
			rep = new StringRepresentation("Success");
		} catch (Throwable t) {
      LOG.error(t);
      rep = new StringRepresentation(cloudataService.getErrorMessage(t),
          MediaType.TEXT_PLAIN);   
		}

		getResponse().setEntity(rep);
	}
}
