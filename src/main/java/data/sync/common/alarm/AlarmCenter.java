package data.sync.common.alarm;

import data.sync.common.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by hesiyuan on 15/7/1.
 */
public class AlarmCenter {
    private static final Log LOG = LogFactory.getLog(AlarmCenter.class);
    private static LinkedBlockingQueue<AlarmMessage> queue = new LinkedBlockingQueue<AlarmMessage>();
    private static Map<String,Alarm> map = new HashMap<String,Alarm>();
    public static void start(Configuration conf){
        List<Alarm> alarms = conf.getInstances("",Alarm.class);
        for(Alarm alarm:alarms){
            map.put(alarm.getAlarmName(),alarm);
        }
        new Thread(){
            @Override
            public void run() {
                for(;;){
                    AlarmMessage message = null;
                    try{
                        message = queue.take();
                        if(message!=null)
                            for(Alarm alarm:map.values()) {
                               sendWithRetry(alarm,message);
                            }
                    }catch(Exception e){
                        LOG.error("message send error",e);
                    }
                }
            }
        }.start();
    }
    public static void addAlarmMessage(AlarmMessage message){
        queue.add(message);
    }
    private static void sendWithRetry(Alarm alarm,AlarmMessage message){
        for(int i=0;i<message.getRetryTimes();i++){
            try {
                alarm.alarm(message);
                break;
            }catch(Exception e){
                LOG.error("message send error :"+i,e);
            }
        }
    }
}
