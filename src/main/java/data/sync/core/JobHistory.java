package data.sync.core;

import data.sync.common.Configuration;
import data.sync.common.Constants;

import static data.sync.common.ClusterMessages.*;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
/**
 * Created by hesiyuan on 15/6/24.
 */
public class JobHistory {
    public static final String HISTORY_DIR;
    static{
        Configuration conf = new Configuration();
        conf.addResource(Constants.CONFIGFILE_NAME);
        HISTORY_DIR = conf.get(Constants.HISTORY_DIR,Constants.HISTORY_DIR_DEFAULT);
    }
    public static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    /*
     * 获得正在运行的一个job
     * 来源JobManager
     */
    public static HJob getMemHjob(String jobId) {
        JobInfo jobInfo = JobManager.getJob(jobId);
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
                    attempts.add(new HAttempt(attempt[i].attemptId(),report.readNum(),report.writeNum(),format.format(new Date(report.time())),report.error(),report.status()));
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
     * 获得正在运行的所有Job
     * 来源JobManager
     */
    public static List<HJob> getAllMemJob(){
        return null;
    }
    /*
     * 获得历史Job
     * 来源文件
     */
    public static List<HJob> getHistoryJob(){
        return null;
    }
    /*
     * 将一个运行完的Job存为文件
     * 来源JobManager
     */
    public static void dumpMemJob(String jobId) throws IOException {
        FileOutputStream fis = new FileOutputStream(HISTORY_DIR+jobId);
        ObjectOutputStream oos = new ObjectOutputStream(fis);
        HJob job = getMemHjob(jobId);
        oos.writeObject(job);
        oos.close();
    }

    public static class HJob implements Serializable {
        private String jobId;
        private int priority;
        private String submitTime;
        private String targetDir;
        private String jobDesc;
        private Set<HTask> tasks;
        private JobStatus status;

        public static HJob generateFromJobInfo(JobInfo ji){
            HJob job = new HJob();
            job.jobId = ji.jobId();
            job.priority = ji.priority();
            job.submitTime = format.format(new Date(ji.submitTime()));
            job.targetDir = ji.targetDir();
            job.jobDesc = "";
            job.status = ji.status();
            return job;
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
            return task;
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
        private String finishTime;
        private String error;
        private TaskAttemptStatus status;
        private long readNum;
        private long writeNum;
        public HAttempt(String attemptId,long readNum, long writeNum, String finishTime, String error, TaskAttemptStatus status) {
            this.attemptId = attemptId;
            this.readNum = readNum;
            this.writeNum = writeNum;
            this.finishTime = finishTime;
            this.error = error;
            this.status = status;
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
