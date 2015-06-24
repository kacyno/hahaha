package data.sync.common;

import java.util.Date;
import java.util.UUID;

/**
 * Created by hesiyuan on 15/6/24.
 */
public class IDGenerator {

    public static String generatorJobId(){
        return "job_"+UUID.randomUUID();
    }
}
