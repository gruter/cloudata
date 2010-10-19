<%@ page
  contentType="text/html; charset=UTF-8"
	import="java.io.*"
	import="java.util.*"
	import="org.apache.commons.fileupload.*"
	import="org.apache.commons.fileupload.disk.*"
	import="org.apache.commons.fileupload.servlet.*"
	import="org.cloudata.core.common.conf.*"
	import="org.cloudata.tools.cloudatafs.*"
%>
<%!
private CloudataConf conf = new CloudataConf();
%>


<%
	request.setCharacterEncoding("UTF-8");
	String parentPath = request.getParameter("parentPath");
	    
	if (!ServletFileUpload.isMultipartContent(request)) {
	  out.println("Invalid request type");
	  return;
	}
	try {
	  DiskFileItemFactory itemFac = new DiskFileItemFactory();
	  itemFac.setSizeThreshold(20000000); // bytes
	  
	  File repositoryPath = new File("upload");
	  repositoryPath.mkdir();
	  itemFac.setRepository(repositoryPath);
	  
	  ServletFileUpload servletFileUpload = new ServletFileUpload(itemFac);
	  List fileItemList = servletFileUpload.parseRequest(request);
	  Iterator list = fileItemList.iterator();
	  while (list.hasNext()) {
	    FileItem item = (FileItem) list.next();
	    String paramName = item.getFieldName();
	    if (paramName.equals("Filedata")) {
	      WebUtil.saveFile(conf, item, parentPath);
	    }
	  }
	  out.print("1");
	} catch (Exception e) {
	  out.println("Internal Server Error");
	  e.printStackTrace();
	  throw e;
	}
%>