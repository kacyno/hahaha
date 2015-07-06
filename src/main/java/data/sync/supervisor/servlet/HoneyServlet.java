package data.sync.supervisor.servlet;

import data.sync.core.JobStatus;
import data.sync.supervisor.alarm.AlarmMessage;
import data.sync.supervisor.main.Supervisor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by hesiyuan on 15/7/3.
 */
public class HoneyServlet extends HttpServlet {
    /**
     *
     */
    private static final long serialVersionUID = -4228529050155695129L;
    private static final Log LOG = LogFactory.getLog(HoneyServlet.class);
    private int sendTimes;
    private Map<String, Map<String, String[]>> alarmMap = new HashMap<String, Map<String, String[]>>();

    @Override
    public void init() throws ServletException {
        super.init();
        sendTimes = Supervisor.conf.getInt(
                "supervisor.honey.messsage.send.times", -1);

        String[] alarms = Supervisor.conf.getStrings("supervisor.honey.alarms");
        String[] groups = Supervisor.conf.getStrings("supervisor.honey.alarm.targets.groups");
        for (int i = 0; i < alarms.length; i++) {
            Map<String, String[]> temp = new HashMap<String, String[]>();
            for (int j = 0; j < groups.length; j++) {
                temp.put(groups[j], Supervisor.conf.getStrings("supervisor.honey.alarm."
                        + alarms[i] + "." + groups[j] + ".targets"));
            }
            alarmMap.put(
                    alarms[i],
                    temp);
        }
    }
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        String jobId = req.getParameter("jobId");
        String status = req.getParameter("status");
        String user = req.getParameter("user");
        String jobName = req.getParameter("jobName");

        if (JobStatus.FAILED.toString().equals(status)) {
            try {
                LOG.info("Begin to alarm");
                Set<String> set = alarmMap.keySet();
                for (String key : set) {
                    Map<String, String[]> groups = alarmMap.get(key);
                    String[] targets = null;
                    for (String group : groups.keySet()) {
                        if (jobName.startsWith(group)) {
                            targets = groups.get(group);
                            break;
                        }
                    }
                    if (targets == null)
                        continue;
                    AlarmMessage message = new AlarmMessage();
                    String title = jobName;
                    message.setRetryTimes(sendTimes);
                    message.setTitle(title );
                    message.setBody(jobId+"::"+user+" "+status);
                    message.setTargets(targets);
                    Supervisor.queueMap.get(key).add(message);
                }
            } catch (Exception e) {
                LOG.error("alarm error", e);
            }

        }else{
            LOG.info(jobName+"  "+jobId+"@"+user+" "+status);
        }
    }
}

