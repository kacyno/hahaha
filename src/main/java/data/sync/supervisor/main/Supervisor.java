package data.sync.supervisor.main;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import javax.servlet.Servlet;
import data.sync.supervisor.alarm.Alarm;
import data.sync.supervisor.alarm.AlarmMessage;
import data.sync.supervisor.alarm.AlarmRunner;
import data.sync.supervisor.servlet.SupervisorServlet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

public class Supervisor {
	private static final Log LOG = LogFactory.getLog(Supervisor.class);
	public static Map<String,Queue<AlarmMessage>>  queueMap= new HashMap<String,Queue<AlarmMessage>>();
	public static Map<String,String> servletInfo = new HashMap<String,String>();
	public static Configuration conf = new Configuration();
	static{
		conf.addResource("supervisor-site.xml");
	}
	public static void main(String[] args) throws Exception {
		Supervisor supervisor = new Supervisor();
		LOG.info("start alarm...");
		supervisor.initAlarm();
		LOG.info("start server...");
		supervisor.initServer();
	}
	public void initAlarm() throws Exception{
		Class<Alarm>[] alarms = (Class<Alarm>[])conf.getClasses("supervisor.alarm.classes");
		for(int i=0;i<alarms.length;i++){
			Alarm alarm = alarms[i].newInstance();
			alarm.init();
			LOG.info("add alarm "+alarm.getQueueName()+" ......");
			new AlarmRunner(alarm).start();
		}
	}
	public void initServer() throws Exception{
		int port = conf.getInt("supervisor.http.port",8044);
		Server server = new Server(port);
		ServletContextHandler servlet = new ServletContextHandler(
				ServletContextHandler.SESSIONS);
		servlet.setContextPath("/supervisor");
		servlet.addServlet(new ServletHolder(SupervisorServlet.class), "/");
		String[] paths = conf.getStrings("supervisor.paths");
		for(int i=0;i<paths.length;i++){
			LOG.info("add path "+paths[i]+" ......");
			servlet.addServlet(new ServletHolder((Servlet)conf.getClass("supervisor."+paths[i]+".servlet",Servlet.class).newInstance()), "/"+paths[i]);
			servletInfo.put("/supervisor/"+paths[i], conf.get("supervisor."+paths[i]+".servlet"));
		}
		ContextHandlerCollection contexts = new ContextHandlerCollection();
		contexts.setHandlers(new Handler[] { servlet});
		server.setHandler(contexts);
		
		LOG.info("Supervisor Start...");
		server.start();
		server.join();
	}
}
