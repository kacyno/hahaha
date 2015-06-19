package data.sync.http.servlet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.DailyRollingFileAppender;
import org.eclipse.jetty.websocket.WebSocket;
import org.eclipse.jetty.websocket.WebSocketServlet;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * 显示当前日志，用于监控
 * Created by hesiyuan on 15/1/30.
 */
public class TailLogServlet extends WebSocketServlet {
    public static final Log LOG = LogFactory.getLog(TailLogServlet.class);

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws javax.servlet.ServletException, IOException {
        getServletContext().getNamedDispatcher("default").forward(request, response);
    }

    @Override
    public WebSocket doWebSocketConnect(HttpServletRequest httpServletRequest, String s) {
        return new TailLogSocket();
    }
}

/**
 * 通过tail -f实现日志时实推送的线程
 */
class Tailor extends Thread {
    public static final Log LOG = LogFactory.getLog(Tailor.class);

    WebSocket.Connection client;
    volatile boolean stop = false;

    public Tailor(WebSocket.Connection client) {
        this.client = client;
    }

    @Override
    public void run() {
        String logPath = ((DailyRollingFileAppender) org.apache.log4j.Logger.getRootLogger().getAppender("DRFA")).getFile();
        String[] command = {"tail", "-F","-n40", logPath};
        BufferedReader in=null;
        Process pro=null;
        String log;
        try {
            client.sendMessage("LOG File Path:"+logPath + "\r\n");
            pro = Runtime.getRuntime().exec(command);
            in = new BufferedReader(new InputStreamReader(pro.getInputStream()));
            while ((log = in.readLine())!=null&&!stop) {
                    client.sendMessage(log+"\r\n");
            }
        } catch (Exception e) {
            LOG.debug("Tail Log error!", e);
        } finally {
            if(pro!=null)
                pro.destroy();
            if(in!=null)
                try {
                    in.close();
                } catch (IOException e) {
                }
            client.close();
        }
    }

    public void over() {
        this.stop = true;
        this.interrupt();
    }
}

/**
 * webSocket实现
 */
class TailLogSocket implements WebSocket, WebSocket.OnTextMessage {

    Tailor tail;

    @Override
    public void onOpen(Connection connection) {
        tail = new Tailor(connection);
        tail.setDaemon(true);
        tail.setName("TailLog Thread");
        tail.start();
    }

    @Override
    public void onClose(int i, String s) {
        tail.over();
    }

    @Override
    public void onMessage(String s) {
        if ("over".equals(s)) {
            tail.over();
        }
    }
}
