package data.sync.http.restful;

import static data.sync.common.ClusterMessages.*;
import data.sync.core.BeeManager;
import data.sync.core.JobHistory;
import data.sync.core.JobManager;
import net.sf.json.JSONArray;
import scala.Tuple2;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.*;

/**
 * Created by hesiyuan on 15/6/26.
 */
@Path("/bee")
public class BeeInfoRest {
    /*
    [
{
beeId: "bee@0.0.0.0:43934",
runningWorker: 0,
totalWorker: 10
},
{
beeId: "bee@0.0.0.0:43936",
runningWorker: 0,
totalWorker: 10
}
]
     */
    @GET
    @Path("/allbees")
    @Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
    public String getBees(){
        return JSONArray.fromObject(BeeManager.allBeesInfo().values()).toString();
    }

/*
[
{
taskId: "job_0ac6cc94-6099-46fb-b73e-f29b99279a77_task_1",
readNum: "5655",
jobId: "job_0ac6cc94-6099-46fb-b73e-f29b99279a77",
status: "RUNNING",
beeId: "bee@0.0.0.0:43934",
writeNum: "5655",
attemptId: "job_0ac6cc94-6099-46fb-b73e-f29b99279a77_task_1-attempt-1"
},
{
taskId: "job_0ac6cc94-6099-46fb-b73e-f29b99279a77_task_3",
readNum: "6923",
jobId: "job_0ac6cc94-6099-46fb-b73e-f29b99279a77",
status: "RUNNING",
beeId: "bee@0.0.0.0:43934",
writeNum: "6924",
attemptId: "job_0ac6cc94-6099-46fb-b73e-f29b99279a77_task_3-attempt-1"
}
]
 */
    @GET
    @Path("/beeinfo")
    @Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
    public String getBee(@QueryParam("beeid") String beeId){
        Set<Tuple2<TaskAttemptInfo,BeeAttemptReport>> set = JobManager.getAttemptsByBee(beeId);
        Set<Map<String,String>> info = new HashSet<Map<String,String>>();
        for(Tuple2<TaskAttemptInfo,BeeAttemptReport> tuple2: set){
            info.add(getBeeAttemptMap(tuple2));
        }
        return JSONArray.fromObject(info).toString();
    }

    @GET
    @Path("/beeinfos")
    @Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
    public String getBee(){
        List<Map<String,String>> list = new ArrayList<Map<String, String>>();
        for(String beeId : BeeManager.allBeesInfo().keySet()) {
            Set<Tuple2<TaskAttemptInfo, BeeAttemptReport>> set = JobManager.getAttemptsByBee(beeId);
            for (Tuple2<TaskAttemptInfo, BeeAttemptReport> tuple2 : set) {
                list.add(getBeeAttemptMap(tuple2));
            }
        }
        return JSONArray.fromObject(list).toString();
    }
    private Map<String,String> getBeeAttemptMap(Tuple2<TaskAttemptInfo, BeeAttemptReport> tuple2){
        Map<String, String> map = new HashMap<String, String>();
        map.put("beeId", tuple2._2().beeId());
        map.put("attemptId", tuple2._1().attemptId());
        map.put("taskId", tuple2._1().taskDesc().taskId());
        map.put("jobId", tuple2._1().taskDesc().jobId());
        map.put("readNum", String.valueOf(tuple2._2().readNum()));
        map.put("bufferSize",String.valueOf(tuple2._2().bufferSize()));
        map.put("writeNum", String.valueOf(tuple2._2().writeNum()));
        map.put("startTime", JobHistory.format.format(new Date(tuple2._1().startTime())));
        map.put("endTime", JobHistory.format.format(new Date(tuple2._1().finishTime())));
        map.put("status", tuple2._1().status().toString());
        return map;
    }
}
