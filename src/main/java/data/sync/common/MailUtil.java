package data.sync.common;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import java.util.Properties;

/**
 * Created by hesiyuan on 14/11/26.
 */
public class MailUtil {
    private Session session;
    private String host;
    private String user;
    private String pwd;
    private String from;

    public MailUtil(String host, String user, String pwd, String from) {
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
