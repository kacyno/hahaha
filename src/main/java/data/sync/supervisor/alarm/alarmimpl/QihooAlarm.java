package data.sync.supervisor.alarm.alarmimpl;

import java.util.HashMap;
import java.util.Map;

import data.sync.common.HttpUtil;
import data.sync.supervisor.alarm.AbstractAlarm;
import data.sync.supervisor.alarm.AlarmMessage;
import data.sync.supervisor.main.Supervisor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class QihooAlarm extends AbstractAlarm {
	private static final Log LOG = LogFactory.getLog(QihooAlarm.class);
	private String url;
	public boolean init() {
		url = Supervisor.conf.get("supervisor.alarm.qihoo.url", "");
		return true;
	}

	public boolean alarm(AlarmMessage message) {
		try {
			String[] targets = message.getTargets();
			for(int i=0;i<targets.length;i++){
				String target = targets[i];
				Map<String, String> map = new HashMap<String, String>();
				map.put("group_name", target);
				map.put("content",message.getBody());
				map.put("subject",message.getTitle());
				HttpUtil.execute(url, map);
			}
			return true;
		} catch (Exception e) {
			LOG.error("Qihoo alarm error", e);
			return false;
		}
	}
}
