package data.sync.http.restful;

import data.sync.core.JobManager;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

/**
 * Created by hesiyuan on 15/6/26.
 */
@Path("/job")
public class JobInfoRest {
    @GET
    @Path("/alljobs")
    @Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
    public String getJobs(){
        return JobManager.getAllJobJson();
    }
    @GET
    @Path("/jobinfo")
    @Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
    public String getJob(@QueryParam("jobid")String jobId){
        return JobManager.getJobJson(jobId);
    }
}
