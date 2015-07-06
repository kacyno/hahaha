package data.sync.supervisor.alarm;

public interface Alarm {
	boolean init();
	boolean alarm(AlarmMessage message);
	String getQueueName();
}
