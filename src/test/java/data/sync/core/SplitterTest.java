package data.sync.core;

import data.sync.common.ClientMessage;
import data.sync.common.ClientMessages;
import org.junit.Test;

/**
 * Created by hesiyuan on 15/6/25.
 */
public class SplitterTest {
    @Test
    public void test1(){
        ClientMessages.DBInfo db = new ClientMessages.DBInfo("select * from %s where 0=0","id",new String[]{"import_cps_confirm_1"},"test","localhost","3306","root","lkmlnfqp");

        System.out.println(SimpleSplitter.split("test",new ClientMessages.DBInfo[]{db},2,"/tmp/"));
    }
}
