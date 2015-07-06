package data.sync.supervisor.alarm.alarmimpl;

import java.util.Properties;

import javax.mail.Address;
import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import data.sync.supervisor.alarm.AbstractAlarm;
import data.sync.supervisor.alarm.AlarmMessage;
import data.sync.supervisor.main.Supervisor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class MailAlarm extends AbstractAlarm {
	private static final Log LOG = LogFactory.getLog(MailAlarm.class);
	private MailUtil mu;
	public boolean init() {
		mu = new MailUtil(Supervisor.conf.get("supervisor.alarm.mail.host"),
				Supervisor.conf.get("supervisor.alarm.mail.user"),
				Supervisor.conf.get("supervisor.alarm.mail.pwd"),
				Supervisor.conf.get("supervisor.alarm.mail.from")
				);
		return true;
	}

	public boolean alarm(AlarmMessage message) {
		try{
		Address[] ads = new Address[message.getTargets().length];
		for(int i=0;i<message.getTargets().length;i++){
			ads[i] = new InternetAddress(message.getTargets()[i]);
		}
		mu.send(message.getTitle(), message.getBody(),
				ads);
		}catch(Exception e){
			LOG.error("mail error",e);
			return false;
		}
		return true;
	}

	public static class MailUtil {
		private Session session;
		private String host;
		private String user;
		private String pwd;
		private String from;

		public MailUtil(String host, String user, String pwd,String from) {
			this.from = from;
			this.host = host;
			this.user = user;
			this.pwd = pwd;
			Properties props = new Properties();
			props.put("mail.smtp.host", host);
			props.put("mail.smtp.auth", "true");
			session = Session.getDefaultInstance(props);
		}

		public void send(String title, String body,
				Address[] addresses) {
			MimeMessage message = new MimeMessage(session);
			try {
				message.setFrom(new InternetAddress(from));
				message.addRecipients(Message.RecipientType.TO, addresses);
				message.setSubject(title);
				Multipart multipart = new MimeMultipart();
				BodyPart contentPart = new MimeBodyPart();
				contentPart.setText(body);
				multipart.addBodyPart(contentPart);
				message.setContent(multipart);
				message.saveChanges();
				Transport transport = session.getTransport("smtp");
				transport.connect(host, user, pwd);
				transport.sendMessage(message, message.getAllRecipients());
				transport.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

}
