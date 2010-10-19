<%@ page
  contentType="text/html; charset=UTF-8"
  import="java.util.*"
  import="java.io.*"
	import="org.cloudata.tools.cloudatafs.*"
  import="org.cloudata.core.common.conf.*"
%>
<%!
private CloudataConf conf = new CloudataConf();
%>

<%
	CloudataFS fs = CloudataFS.get(conf);
	
  String dir = request.getParameter("dir");
  String sortField = request.getParameter("sidx");
  String orderBy = request.getParameter("sord");
  
  //System.out.println("FileList:" + dir + ">" + sortField + ">" + orderBy);
	out.print(WebUtil.listDir(fs, dir, sortField, orderBy));
%>