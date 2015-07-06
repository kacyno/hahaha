package data.sync.core;

import data.sync.common.ClusterMessages;
import data.sync.common.NetUtil;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by hesiyuan on 15/6/26.
 */
public class CommonTest {
    @Test
    public void test1(){
        Properties prop = System.getProperties();
        System.out.println(NetUtil.getHostname());
// 获取用户名
        System.out.println("\n当前用户名:"+prop.getProperty("user.name"));
    }
}
