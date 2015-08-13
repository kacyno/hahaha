package data.sync.core;

import data.sync.common.ClientMessage;
import data.sync.common.ClientMessages;
import data.sync.common.ClusterMessages;
import org.junit.Test;

import java.util.Set;

/**
 * Created by hesiyuan on 15/6/25.
 */
public class SplitterTest {
    @Test
    public void test1(){
//        ClientMessages.DBInfo db1 = new ClientMessages.DBInfo("select * from %s where 0=0","id",new String[]{"import_cps_confirm"},"test","localhost","3306","root","lkmlnfqp");
//        ClientMessages.DBInfo db2= new ClientMessages.DBInfo("select * from %s where 0=0","id",new String[]{"import_cps_confirm"},"test1","localhost","3306","root","lkmlnfqp");
//
//        Set<ClusterMessages.TaskInfo> set=SimpleSplitter.split("test",new ClientMessages.DBInfo[]{db1,db2},3,"/tmp/");
//        System.out.println(set.size());
//        System.out.println(set);
    }
}
