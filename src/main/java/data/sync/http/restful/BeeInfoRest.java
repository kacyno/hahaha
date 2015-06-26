package data.sync.http.restful;

import static data.sync.common.ClusterMessages.*;
import data.sync.core.BeeManager;
import data.sync.core.JobManager;
import net.sf.json.JSONArray;
import scala.Tuple2;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
        return BeeManager.getAllBeesJson();
    }


    @GET
    @Path("/beeinfo")
    @Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
    public String getBee(@QueryParam("beeid") String beeId){
        Set<Tuple2<TaskAttemptInfo,BeeAttemptReport>> set = JobManager.getAttemptsByBee(beeId);
        Set<Map<String,String>> info = new HashSet<Map<String,String>>();
        for(Tuple2<TaskAttemptInfo,BeeAttemptReport> tuple2: set){
            Map<String,String> map = new HashMap<String,String>();
            map.put("beeId",beeId);
            map.put("attemptId",tuple2._1().attemptId());
            map.put("taskId",tuple2._1().taskDesc().taskId());
            map.put("jobId",tuple2._1().taskDesc().jobId());
            map.put("readNum",String.valueOf(tuple2._2().readNum()));
            map.put("writeNum",String.valueOf(tuple2._2().writeNum()));
            map.put("status",tuple2._1().status().toString());
            info.add(map);
        }

        return JSONArray.fromObject(info).toString();
    }

}
