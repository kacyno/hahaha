package data.sync.supervisor.servlet;

import data.sync.supervisor.main.Supervisor;

import java.io.IOException;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;



public class SupervisorServlet extends HttpServlet{
	 /**
	 * 
	 */
	private static final long serialVersionUID = 8405281974909083549L;

	@Override
	    protected void doGet(HttpServletRequest request,
	           HttpServletResponse response) throws ServletException, IOException{
	       response.setContentType("text/html;charset=utf-8"); // 加上此行代码，避免出现中文乱码
	       response.setStatus(HttpServletResponse.SC_OK);
	       StringBuilder sb = new StringBuilder();
	       sb.append("<font color='red'><h2> Supervisor&nbspInfo </h2></font> &nbsp&nbsp  <br>");
	       
	       sb.append("<hr><h3> Servlet&nbspInfo </h3> &nbsp&nbsp  ");
	       Set<String> servletKeySet = Supervisor.servletInfo.keySet();
	       for(String key:servletKeySet){
	    	   sb.append("<br><bold>【Path】</bold>："+key+"&nbsp&nbsp<bold>【Class】</bold>："+Supervisor.servletInfo.get(key)+"<br>");
	       }
	       sb.append("<br><br><hr><h3> Alarm&nbspInfo </h3> &nbsp&nbsp");
	       Set<String> set = Supervisor.queueMap.keySet();
	       for(String key:set){
	    	   sb.append("<br><bold>【Name】</bold>："+key+"&nbsp&nbsp<bold>【QueueSize】</bold>："+Supervisor.queueMap.get(key).size()+"<br>");
	       }
	       
	       response.getWriter().println(sb.toString());
	    }
}
