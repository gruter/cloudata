<%@ page
  contentType="text/html; charset=UTF-8"
	import="java.io.*"
	import="java.util.*"
%>
<%
	String sleepTime = request.getParameter("time");
	if(sleepTime == null || sleepTime.length() == 0) {
	  sleepTime = "2000";
	}
	Thread.sleep(Integer.parseInt(sleepTime));
%>