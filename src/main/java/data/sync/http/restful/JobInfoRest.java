package data.sync.http.restful;

import data.sync.common.ClusterMessages;
import data.sync.core.JobHistory;
import data.sync.core.JobManager;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.lang.ArrayUtils;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.*;

/**
 * Created by hesiyuan on 15/6/26.
 */
@Path("/job")
public class JobInfoRest {
    /*
[
{
appendTasks: [ ],
finishedTasks: [
{
db: "test",
ip: "localhost",
jobId: "job_0ac6cc94-6099-46fb-b73e-f29b99279a77",
port: "3306",
pwd: "lkmlnfqp",
sql: "select * from import_cps_confirm_1 where 0=0 and id>=824350 and id<924349",
status: "FINSHED",
table: "import_cps_confirm_1",
targetDir: "/Users/hesiyuan/honey-data/tmp/",
taskId: "job_0ac6cc94-6099-46fb-b73e-f29b99279a77_task_3",
user: "root"
},
{
db: "test",
ip: "localhost",
jobId: "job_0ac6cc94-6099-46fb-b73e-f29b99279a77",
port: "3306",
pwd: "lkmlnfqp",
sql: "select * from import_cps_confirm_1 where 0=0 and id>=924349 and id<1024352",
status: "FINSHED",
table: "import_cps_confirm_1",
targetDir: "/Users/hesiyuan/honey-data/tmp/",
taskId: "job_0ac6cc94-6099-46fb-b73e-f29b99279a77_task_4",
user: "root"
},
{
db: "test",
ip: "localhost",
jobId: "job_0ac6cc94-6099-46fb-b73e-f29b99279a77",
port: "3306",
pwd: "lkmlnfqp",
sql: "select * from import_cps_confirm_1 where 0=0 and id>=624352 and id<724351",
status: "FINSHED",
table: "import_cps_confirm_1",
targetDir: "/Users/hesiyuan/honey-data/tmp/",
taskId: "job_0ac6cc94-6099-46fb-b73e-f29b99279a77_task_1",
user: "root"
},
{
db: "test",
ip: "localhost",
jobId: "job_0ac6cc94-6099-46fb-b73e-f29b99279a77",
port: "3306",
pwd: "lkmlnfqp",
sql: "select * from import_cps_confirm_1 where 0=0 and id>=724351 and id<824350",
status: "FINSHED",
table: "import_cps_confirm_1",
targetDir: "/Users/hesiyuan/honey-data/tmp/",
taskId: "job_0ac6cc94-6099-46fb-b73e-f29b99279a77_task_2",
user: "root"
},
{
db: "test",
ip: "localhost",
jobId: "job_0ac6cc94-6099-46fb-b73e-f29b99279a77",
port: "3306",
pwd: "lkmlnfqp",
sql: "select * from import_cps_confirm_1 where 0=0 and id>=524353 and id<624352",
status: "FINSHED",
table: "import_cps_confirm_1",
targetDir: "/Users/hesiyuan/honey-data/tmp/",
taskId: "job_0ac6cc94-6099-46fb-b73e-f29b99279a77_task_0",
user: "root"
}
],
info: [
{ }
],
jobId: "job_0ac6cc94-6099-46fb-b73e-f29b99279a77",
priority: 1,
runningTasks: [ ],
status: "FINSHED",
submitTime: 1435312259251,
targetDir: "/Users/hesiyuan/honey-data/"
}
]
     */
    @GET
    @Path("/alljobs")
    @Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
    public String getJobs(){
        List<Map<String,String>> list = new ArrayList<Map<String,String>>();
        for(ClusterMessages.JobInfo job:JobManager.allJobs().values()){
            Map<String,String> map = new HashMap<String,String>();
            map.put("jobId",job.getJobId());
            map.put("jobDesc", ArrayUtils.toString(job.dbinfos()));
            map.put("priority",String.valueOf(job.priority()));
            map.put("appendTasks",String.valueOf(job.appendTasks().size()));
            map.put("runningTasks",String.valueOf(job.runningTasks().size()));
            map.put("finishedTasks",String.valueOf(job.finishedTasks().size()));
            map.put("startTime",JobHistory.format.format(new Date(job.submitTime())));
            map.put("targetDir",job.getTargetDir());
            map.put("status",job.getStatus().toString());
            list.add(map);
        }
        return JSONArray.fromObject(list).toString();
    }


    @GET
    @Path("/hisjobs")
    @Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
    public String getHisJobs(){
        List<JobHistory.HJob> jobs = JobHistory.getHistoryJob();
        List<Map<String,String>> list = new ArrayList<Map<String,String>>();
        for(JobHistory.HJob job:jobs){
            Map<String,String> map = new HashMap<String,String>();
            map.put("jobId",job.getJobId());
            map.put("jobDesc",job.getJobDesc());
            map.put("startTime",job.getSubmitTime());
            map.put("finishedTime",job.getFinishTime());
            map.put("targetDir",job.getTargetDir());
            map.put("priority",String.valueOf(job.getPriority()));
            map.put("status",job.getStatus().toString());
            list.add(map);
        }
        Collections.reverse(list);
        return JSONArray.fromObject(list).toString();
    }


    /*
    {
jobDesc: "",
jobId: "job_0ac6cc94-6099-46fb-b73e-f29b99279a77",
priority: 1,
status: "FINSHED",
submitTime: "2015-06-26 17:50:59",
targetDir: "/Users/hesiyuan/honey-data/",
tasks: [
{
attempts: [
{
attemptId: "job_0ac6cc94-6099-46fb-b73e-f29b99279a77_task_2-attempt-1",
error: "",
finishTime: "2015-06-26 17:51:11",
readNum: 99998,
status: "FINSHED",
writeNum: 99999
}
],
db: "test",
ip: "localhost",
port: "3306",
pwd: "lkmlnfqp",
sql: "select * from import_cps_confirm_1 where 0=0 and id>=724351 and id<824350",
status: "FINSHED",
table: "import_cps_confirm_1",
targetDir: "/Users/hesiyuan/honey-data/tmp/",
taskId: "job_0ac6cc94-6099-46fb-b73e-f29b99279a77_task_2",
user: "root"
},
{
attempts: [
{
attemptId: "job_0ac6cc94-6099-46fb-b73e-f29b99279a77_task_4-attempt-1",
error: "",
finishTime: "2015-06-26 17:51:11",
readNum: 100002,
status: "FINSHED",
writeNum: 100001
}
],
db: "test",
ip: "localhost",
port: "3306",
pwd: "lkmlnfqp",
sql: "select * from import_cps_confirm_1 where 0=0 and id>=924349 and id<1024352",
status: "FINSHED",
table: "import_cps_confirm_1",
targetDir: "/Users/hesiyuan/honey-data/tmp/",
taskId: "job_0ac6cc94-6099-46fb-b73e-f29b99279a77_task_4",
user: "root"
},
{
attempts: [
{
attemptId: "job_0ac6cc94-6099-46fb-b73e-f29b99279a77_task_3-attempt-1",
error: "",
finishTime: "2015-06-26 17:51:11",
readNum: 99998,
status: "FINSHED",
writeNum: 99999
}
],
db: "test",
ip: "localhost",
port: "3306",
pwd: "lkmlnfqp",
sql: "select * from import_cps_confirm_1 where 0=0 and id>=824350 and id<924349",
status: "FINSHED",
table: "import_cps_confirm_1",
targetDir: "/Users/hesiyuan/honey-data/tmp/",
taskId: "job_0ac6cc94-6099-46fb-b73e-f29b99279a77_task_3",
user: "root"
},
{
attempts: [
{
attemptId: "job_0ac6cc94-6099-46fb-b73e-f29b99279a77_task_1-attempt-1",
error: "",
finishTime: "2015-06-26 17:51:11",
readNum: 99998,
status: "FINSHED",
writeNum: 99999
}
],
db: "test",
ip: "localhost",
port: "3306",
pwd: "lkmlnfqp",
sql: "select * from import_cps_confirm_1 where 0=0 and id>=624352 and id<724351",
status: "FINSHED",
table: "import_cps_confirm_1",
targetDir: "/Users/hesiyuan/honey-data/tmp/",
taskId: "job_0ac6cc94-6099-46fb-b73e-f29b99279a77_task_1",
user: "root"
},
{
attempts: [
{
attemptId: "job_0ac6cc94-6099-46fb-b73e-f29b99279a77_task_0-attempt-1",
error: "",
finishTime: "2015-06-26 17:51:11",
readNum: 99998,
status: "FINSHED",
writeNum: 99999
}
],
db: "test",
ip: "localhost",
port: "3306",
pwd: "lkmlnfqp",
sql: "select * from import_cps_confirm_1 where 0=0 and id>=524353 and id<624352",
status: "FINSHED",
table: "import_cps_confirm_1",
targetDir: "/Users/hesiyuan/honey-data/tmp/",
taskId: "job_0ac6cc94-6099-46fb-b73e-f29b99279a77_task_0",
user: "root"
}
]
}
     */
    @GET
    @Path("/jobinfo")
    @Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
    public String getJob(@QueryParam("jobid")String jobId){
        return JSONObject.fromObject(JobHistory.getMemHjob(jobId)).toString();
    }
}
