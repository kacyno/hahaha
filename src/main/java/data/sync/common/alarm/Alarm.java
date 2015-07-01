package data.sync.common.alarm;

import data.sync.common.Configuration;

public interface Alarm {
	boolean init(Configuration conf);
	boolean alarm(AlarmMessage message);
	String getAlarmName();
}
