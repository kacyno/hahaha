package data.sync.common.alarm;

public abstract class AbstractAlarm implements Alarm {
	public String getAlarmName() {
		return this.getClass().getSimpleName();
	}

}
