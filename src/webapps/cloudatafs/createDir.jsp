<%@ page
  contentType="text/html; charset=UTF-8"
	import="java.io.*"
	import="java.util.*"
	import="org.cloudata.core.common.conf.*"
	import="org.cloudata.tools.cloudatafs.*"
%>

<%!
private CloudataConf conf = new CloudataConf();
%>

<%
	try {
		String parentDir = request.getParameter("parentDir");
		String dir = request.getParameter("dir");
		
		parentDir = CloudataFS.normalizePath(parentDir);
		
		CloudataFS fs = CloudataFS.get(conf);
		
		String path = parentDir + "/" + dir;
		
		if("/".equals(parentDir)) {
			path = parentDir + dir;
		}
		
		fs.mkdir(path);
		out.print(path + " created");
	} catch (Exception e) {
%>
		<%=e.getMessage()%>
<%
	}
%>