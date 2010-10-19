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
		String dir = request.getParameter("dir");
		
		CloudataFS fs = CloudataFS.get(conf);
		
		if("/".equals(dir)) {
		  out.println("can't delete root");
		  return;
		}
		
		fs.delete(dir, true);
		out.print(dir + " deleted");
	} catch (Exception e) {
%>
		<%=e.getMessage()%>
<%
	}
%>