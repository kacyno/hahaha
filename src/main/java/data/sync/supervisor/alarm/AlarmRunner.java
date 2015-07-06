package data.sync.supervisor.alarm;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import data.sync.supervisor.main.Supervisor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class AlarmRunner extends Thread {
	private static final Log LOG = LogFactory.getLog(AlarmRunner.class);
	private Alarm alarm;
	private BlockingQueue<AlarmMessage> queue;
	public AlarmRunner(){}
	public AlarmRunner(Alarm alarm){
		this.alarm = alarm;
		queue = new LinkedBlockingQueue<AlarmMessage>();
		Supervisor.queueMap.put(alarm.getQueueName(), queue);
		this.setDaemon(true);
	}
	@Override
	public void run() {
		for(;;){
			AlarmMessage message = null;
			try{
			message = queue.take();
			if(message!=null)
				if(!alarm.alarm(message)){
					LOG.warn( "【"+message.getTitle()+" "+message.getRetryTimes()+"】");
					if(message.getRetryTimes()!=0){
						message.setRetryTimes(message.getRetryTimes()-1);
						queue.add(message);
					}
				}
			}catch(Exception e){
				LOG.error("message send error",e);
			}
		}
	}
	
}
