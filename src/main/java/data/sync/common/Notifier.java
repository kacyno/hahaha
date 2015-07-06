package data.sync.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by hesiyuan on 15/7/3.
 */
public class Notifier {
    private static final Log LOG = LogFactory.getLog(Notifier.class);
    private static LinkedBlockingQueue<Notify> queue = new LinkedBlockingQueue<Notify>();

    public static void addNotify(String message,Map<String,String> params,int times){
        queue.add(new Notify(message,times,params));
    }
    public static void notifyJob(ClusterMessages.JobInfo job){
        if(org.apache.commons.lang.StringUtils.isNotEmpty(job.notifyUrl())){
            Map<String,String> map = new HashMap<String,String>();
            map.put("jobId",job.getJobId());
            map.put("status", job.getStatus().toString());
            map.put("jobName",job.jobName());
            map.put("user",job.getUser());
            try {
                LOG.info("Notify:"+map);
                Notifier.addNotify(job.notifyUrl(), map,3);
            }catch(Exception e){
                LOG.error("Notify error "+job.jobId(),e);
            }
        }
    }
    static class Notify {
        private String message;
        private int times;
        private Map<String,String> params;
        public Notify(String message, int times,Map<String,String> params){
            this.message = message;
            this.times = times;
            this.params = params;
        }
    }
    public static void start() {
        new Thread() {
            @Override
            public void run() {
                for (; ; ) {
                    Notify message = null;
                    try {
                        message = queue.take();
                        sendWithRetry(message);
                    } catch (Exception e) {
                        LOG.error("Message send error", e);
                    }
                }
            }
        }.start();
    }
    private static void sendWithRetry(Notify message) {
        for (int i = 0; i < message.times; i++) {
            try {
                HttpUtil.execute(message.message,message.params);
                break;
            } catch (Exception e) {
                LOG.error("Message send error :" + i, e);
            }
        }
    }
}
