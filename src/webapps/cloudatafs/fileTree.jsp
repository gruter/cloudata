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
/**
  * jQuery File Tree JSP Connector
  * Version 1.0
  * Copyright 2008 Joshua Gould
  * 21 April 2008
*/
	CloudataFS fs = CloudataFS.get(conf);
	
  String dir = request.getParameter("dir");
	
  //System.out.println("dir:" + dir);
  
  if("_root_/".equals(dir)) {
    String path = "/";
    out.print("<ul class=\"jqueryFileTree\" style=\"display: none;\">");
		out.print("<li class=\"directory collapsed\"><a href=\"#\" rel=\"/\">/neptune_fs</a></li>");
    out.print("</ul>");
    return;
  }  
  
  if("/".equals(dir)) {
  } else if (dir.charAt(dir.length()-1) == '\\') {
    dir = dir.substring(0, dir.length()-1);
  }
  if (fs.exists(dir)) {
	  String[] paths = fs.listPath(dir);
	  if(paths != null) {
			Arrays.sort(paths, String.CASE_INSENSITIVE_ORDER);
			out.print("<ul class=\"jqueryFileTree\" style=\"display: none;\">");
			// All dirs
			for (String path : paths) {
		    if (fs.isDir(path)) {
		      String pathName = CloudataFS.getPathName(path);
					out.print("<li class=\"directory collapsed\"><a href=\"#\" rel=\"" + path + "/\">"
						+ pathName + "</a></li>");
		    }
			}
			// All files
			/*
			for (String path : paths) {
		    if (!fs.isDir(path)) {
		      String pathName = NeptuneFS.getPathName(path);
					int dotIndex = pathName.lastIndexOf('.');
					String ext = dotIndex > 0 ? pathName.substring(dotIndex + 1) : "";
					out.print("<li class=\"file ext_" + ext + "\"><a href=\"#\" rel=\"" + path + "\">"
						+ pathName + "</a></li>");
	    	}
			}
			*/
	  }
		out.print("</ul>");
	}
%>