package data.sync.core;

import data.sync.common.ClusterMessages;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by hesiyuan on 15/6/26.
 */
public class CommonTest {
    @Test
    public void test1(){
        Map<String,String> map = Collections.synchronizedMap(new LinkedHashMap<String,String>(1000, 0.15f, true));
        map.put("1","1");
        map.put("2","2");
        map.get("1");
        System.out.println(map);
    }
}
