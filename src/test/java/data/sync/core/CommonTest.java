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
        Set<ClusterMessages.TaskInfo> set = new HashSet<ClusterMessages.TaskInfo>();
        ClusterMessages.TaskInfo b = new ClusterMessages.TaskInfo("","","","","","","","","","",TaskStatus.FAILED);
        System.out.println(b.hashCode());
        ClusterMessages.TaskInfo a = new ClusterMessages.TaskInfo("","","","","","","","","","",TaskStatus.FAILED);
        System.out.println(b.hashCode());
        System.out.println((a.equals(b)));
        set.add(a);
        set.add(b);
        System.out.println(set.size());
        set.remove(a);
        System.out.println(set.size());
    }
}
