package data.sync.core;

import data.sync.common.Configuration;
import data.sync.common.Constants;
import org.apache.commons.lang.ArrayUtils;

import static data.sync.common.ClusterMessages.*;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by hesiyuan on 15/6/24.
 */
public class JobHistory {
    public static final String HISTORY_DIR;
    public static final String POST_FIX = ".hisjob";
    static{
        Configuration conf = new Configuration();
        conf.addResource(Constants.CONFIGFILE_NAME);
        HISTORY_DIR = conf.get(Constants.HISTORY_DIR,Constants.HISTORY_DIR_DEFAULT);
    }
    public static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static Map<String,HJob> jobHistorys = new LinkedHashMap<String,HJob>(1000, 0.15f);
    public static synchronized void  addJobToHistory(String jobId) throws IOException {
        HJob job = getMemHjob(jobId);
        if(jobHistorys.size()>=1000){
            Iterator<String> ite = jobHistorys.keySet().iterator();
            for(int i=0;i<50;i++){
                    jobHistorys.remove(ite.next());
            }
        }
        jobHistorys.put(job.jobId,job);
        dumpMemJob(job);
    }
    /*
     * 获得正在运行的一个job
     * 来源JobManager
     */
    public static HJob getMemHjob(String jobId) {
        JobInfo jobInfo = JobManager.getJob(jobId);
        if(jobInfo==null)
            synchronized(JobHistory.class) {
                return jobHistorys.get(jobId);
            }
        Set<TaskInfo> tasks = new HashSet<TaskInfo>();
        tasks.addAll(jobInfo.appendTasks());
        tasks.addAll(jobInfo.finishedTasks());
        tasks.addAll(jobInfo.runningTasks());
        Set<HTask> htasks = new HashSet<HTask>();
        for(TaskInfo task:tasks){
            TaskAttemptInfo[] attempt = JobManager.getAttempts(task.taskId());
            Set<HAttempt> attempts = new HashSet<HAttempt>();
            if(attempt.length>0) {
                for (int i = 0; i < attempt.length; i++) {
                    BeeAttemptReport report = JobManager.getReport(attempt[i].attemptId());
                    attempts.add(new HAttempt(attempt[i].attemptId(),report.beeId(),report.readNum(),report.writeNum(),format.format(new Date(attempt[i].startTime())),format.format(new Date(attempt[i].finishTime())),report.error(),attempt[i].status()));
                }
            }
            HTask htask = HTask.generateFormTaskInfo(task);
            htask.setAttempts(attempts);
            htasks.add(htask);
        }
        HJob job = HJob.generateFromJobInfo(jobInfo);
        job.setTasks(htasks);
        return job;
    }
    /*
     * 获得历史Job
     */
    public static synchronized List<HJob>  getHistoryJob(){
        List<HJob> jobs = new ArrayList<HJob>();
        for(String key:jobHistorys.keySet()){
            jobs.add(jobHistorys.get(key));
        }
        return jobs;
    }
    /*
     * 将一个运行完的Job存为文件
     * 来源JobManager
     */
    public static void dumpMemJob(HJob job) throws IOException {
        FileOutputStream fis = new FileOutputStream(HISTORY_DIR+job.jobId+POST_FIX);
        ObjectOutputStream oos = new ObjectOutputStream(fis);
        oos.writeObject(job);
        oos.close();
    }

    /*
     *
     */
    public static synchronized void init() throws IOException, ClassNotFoundException {
        File f = new File(HISTORY_DIR);
        File[] files = f.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(POST_FIX);
            }
        });
        Arrays.sort(files,new Comparator<File>(){
            @Override
            public int compare(File o1, File o2) {
                return (int)(o2.lastModified()-o1.lastModified());
            }
        });
        int length = files.length>1000?1000:files.length;
        for(int i=length;i>0;i--){
            ObjectInputStream ois = new ObjectInputStream(new FileInputStream(files[i-1]));
            HJob job = (HJob)ois.readObject();
            jobHistorys.put(job.jobId,job);
            ois.close();
        }
    }
















    public static class HJob implements Serializable {
        private String jobId;
        private int priority;
        private String submitTime;
        private String finishTime;
        private String targetDir;
        private String jobDesc;
        private Set<HTask> tasks;
        private JobStatus status;

        public static HJob generateFromJobInfo(JobInfo ji){
            HJob job = new HJob();
            job.jobId = ji.jobId();
            job.priority = ji.priority();
            job.submitTime = format.format(new Date(ji.submitTime()));
            job.finishTime = format.format(new Date(ji.finishTime()));
            job.targetDir = ji.targetDir();
            job.jobDesc = ArrayUtils.toString(ji.info());
            job.status = ji.status();
            return job;
        }
        public String getFinishTime() {
            return finishTime;
        }

        public void setFinishTime(String finishTime) {
            this.finishTime = finishTime;
        }
        public String getJobId() {
            return jobId;
        }

        public void setJobId(String jobId) {
            this.jobId = jobId;
        }

        public int getPriority() {
            return priority;
        }

        public void setPriority(int priority) {
            this.priority = priority;
        }

        public String getSubmitTime() {
            return submitTime;
        }

        public void setSubmitTime(String submitTime) {
            this.submitTime = submitTime;
        }

        public String getTargetDir() {
            return targetDir;
        }

        public void setTargetDir(String targetDir) {
            this.targetDir = targetDir;
        }

        public String getJobDesc() {
            return jobDesc;
        }

        public void setJobDesc(String jobDesc) {
            this.jobDesc = jobDesc;
        }

        public Set<HTask> getTasks() {
            return tasks;
        }

        public void setTasks(Set<HTask> tasks) {
            this.tasks = tasks;
        }

        public JobStatus getStatus() {
            return status;
        }

        public void setStatus(JobStatus status) {
            this.status = status;
        }
    }

    public static class HTask implements Serializable {
        private String taskId;
        private String sql;
        private String ip;
        private String port;
        private String user;
        private String pwd;
        private String db;
        private String table;
        private String targetDir;
        private String startTime;
        private String finishTime;
        private Set<HAttempt> attempts;
        private TaskStatus status;

        public static HTask generateFormTaskInfo(TaskInfo ti){
            HTask task = new HTask();
            task.taskId = ti.taskId();
            task.sql = ti.sql();
            task.ip = ti.ip();
            task.port = ti.port();
            task.user = ti.user();
            task.pwd = ti.pwd();
            task.db = ti.db();
            task.table = ti.table();
            task.targetDir = ti.targetDir();
            task.status = ti.status();
            task.startTime = format.format(new Date(ti.startTime()));
            task.finishTime = format.format(new Date(ti.finishTime()));
            return task;
        }

        public String getStartTime() {
            return startTime;
        }

        public void setStartTime(String startTime) {
            this.startTime = startTime;
        }

        public String getFinishTime() {
            return finishTime;
        }

        public void setFinishTime(String finishTime) {
            this.finishTime = finishTime;
        }

        public String getTaskId() {
            return taskId;
        }

        public void setTaskId(String taskId) {
            this.taskId = taskId;
        }

        public String getSql() {
            return sql;
        }

        public void setSql(String sql) {
            this.sql = sql;
        }

        public String getIp() {
            return ip;
        }

        public void setIp(String ip) {
            this.ip = ip;
        }

        public String getPort() {
            return port;
        }

        public void setPort(String port) {
            this.port = port;
        }

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }

        public String getPwd() {
            return pwd;
        }

        public void setPwd(String pwd) {
            this.pwd = pwd;
        }

        public String getDb() {
            return db;
        }

        public void setDb(String db) {
            this.db = db;
        }

        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
        }

        public String getTargetDir() {
            return targetDir;
        }

        public void setTargetDir(String targetDir) {
            this.targetDir = targetDir;
        }

        public Set<HAttempt> getAttempts() {
            return attempts;
        }

        public void setAttempts(Set<HAttempt> attempts) {
            this.attempts = attempts;
        }

        public TaskStatus getStatus() {
            return status;
        }

        public void setStatus(TaskStatus status) {
            this.status = status;
        }
    }

    public static class HAttempt implements Serializable {
        private String attemptId;
        private String startTime;
        private String finishTime;
        private String error;
        private String beeId;
        private TaskAttemptStatus status;
        private long readNum;
        private long writeNum;
        public HAttempt(String attemptId,String beeId,long readNum, long writeNum,String startTime, String finishTime, String error, TaskAttemptStatus status) {
            this.attemptId = attemptId;
            this.readNum = readNum;
            this.writeNum = writeNum;
            this.startTime = startTime;
            this.finishTime = finishTime;
            this.beeId = beeId;
            this.error = error;
            this.status = status;
        }

        public String getBeeId() {
            return beeId;
        }

        public void setBeeId(String beeId) {
            this.beeId = beeId;
        }

        public String getStartTime() {
            return startTime;
        }

        public void setStartTime(String startTime) {
            this.startTime = startTime;
        }

        public long getReadNum() {
            return readNum;
        }

        public void setReadNum(long readNum) {
            this.readNum = readNum;
        }

        public long getWriteNum() {
            return writeNum;
        }

        public void setWriteNum(long writeNum) {
            this.writeNum = writeNum;
        }

        public String getAttemptId() {
            return attemptId;
        }

        public void setAttemptId(String attemptId) {
            this.attemptId = attemptId;
        }


        public String getFinishTime() {
            return finishTime;
        }

        public void setFinishTime(String finishTime) {
            this.finishTime = finishTime;
        }

        public String getError() {
            return error;
        }

        public void setError(String error) {
            this.error = error;
        }

        public TaskAttemptStatus getStatus() {
            return status;
        }

        public void setStatus(TaskAttemptStatus status) {
            this.status = status;
        }
    }
}
