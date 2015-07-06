package data.sync.supervisor.alarm;

public class AlarmMessage {
	private String title;
	private String body;
	private String[] targets;
	private int retryTimes=5;
	public AlarmMessage(){}
	public AlarmMessage(String title, String body, String[] targets,int retryTimes) {
		super();
		this.title = title;
		this.body = body;
		this.targets = targets;
		this.retryTimes = retryTimes;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getBody() {
		return body;
	}
	public void setBody(String body) {
		this.body = body;
	}
	public String[] getTargets() {
		return targets;
	}
	public void setTargets(String[] targets) {
		this.targets = targets;
	}
	public int getRetryTimes() {
		return retryTimes;
	}
	public void setRetryTimes(int retryTimes) {
		this.retryTimes = retryTimes;
	}
	
}
