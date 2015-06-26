package data.sync.core;

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by hesiyuan on 15/6/26.
 */
public class CommonTest {
    @Test
    public void test1(){
        Map<Integer,Integer> map = new HashMap<Integer, Integer>();
        map = Collections.synchronizedMap(map);
        map.put(1,1);
        map.put(2,2);
        for(Integer k : map.keySet()){
            map.remove(1);
            System.out.println(map);
        }
    }
}
