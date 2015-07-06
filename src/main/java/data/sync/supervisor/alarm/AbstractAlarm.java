package data.sync.supervisor.alarm;

public abstract class AbstractAlarm implements Alarm {
	public String getQueueName() {
		return this.getClass().getSimpleName();
	}

}
