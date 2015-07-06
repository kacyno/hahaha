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
    @GET
    @Path("/alljobs")
    @Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
    public String getJobs(){
        List<Map<String,String>> list = new ArrayList<Map<String,String>>();
        for(ClusterMessages.JobInfo job:JobManager.allJobs().values()){
            Map<String,String> map = new HashMap<String,String>();
            map.put("jobId",job.getJobId());
            map.put("jobDesc", JobHistory.getSimpleJobDesc(job));
            map.put("priority",String.valueOf(job.priority()));
            map.put("appendTasks",String.valueOf(job.appendTasks().size()));
            map.put("runningTasks",String.valueOf(job.runningTasks().size()));
            map.put("finishedTasks",String.valueOf(job.finishedTasks().size()));
            map.put("failedTasks",String.valueOf(job.failedTasks().size()));
            map.put("startTime",JobHistory.format.format(new Date(job.submitTime())));
            map.put("targetDir",job.getTargetDir());
            map.put("jobName",job.jobName());
            map.put("user",job.getUser());
            map.put("cmd",job.callbackCMD());
            map.put("url",job.notifyUrl());
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
            map.put("jobName",job.getJobname());
            map.put("user",job.getUser());
            map.put("cmd",job.getCmd());
            map.put("url",job.getUrl());
            map.put("status",job.getStatus().toString());
            list.add(map);
        }
        Collections.reverse(list);
        return JSONArray.fromObject(list).toString();
    }


    @GET
    @Path("/jobinfo")
    @Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
    public String getJob(@QueryParam("jobid")String jobId){
        return JSONObject.fromObject(JobHistory.getMemHjob(jobId)).toString();
    }

}
