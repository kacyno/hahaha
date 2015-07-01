package data.sync.common.alarm;


import javax.mail.Address;
import javax.mail.internet.InternetAddress;

import data.sync.common.Configuration;
import data.sync.common.MailUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MailAlarm extends AbstractAlarm {
	private static final Log LOG = LogFactory.getLog(MailAlarm.class);
	private MailUtil mu;
	private String[] targets;
	public boolean init(Configuration conf) {
		mu = new MailUtil(conf.get("supervisor.alarm.mail.host"),
				conf.get("supervisor.alarm.mail.user"),
				conf.get("supervisor.alarm.mail.pwd"),
				conf.get("supervisor.alarm.mail.from")
				);
		targets = conf.getStrings("");
		return true;
	}

	public boolean alarm(AlarmMessage message) {
		try{
		Address[] ads = new Address[targets.length];
		for(int i=0;i<targets.length;i++){
			ads[i] = new InternetAddress(targets[i]);
		}
		mu.send(message.getTitle(), message.getBody(),
				ads);
		}catch(Exception e){
			LOG.error("mail error",e);
			return false;
		}
		return true;
	}

}
