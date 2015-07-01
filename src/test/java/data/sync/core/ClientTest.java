package data.sync.core;

import data.sync.client.HoneyClient;
import data.sync.common.ClientMessages;
import org.junit.Test;

/**
 * Created by hesiyuan on 15/7/1.
 */
public class ClientTest {
    @Test
    public void test1() {
        ClientMessages.SubmitJob job = new ClientMessages.SubmitJob(1,
                new ClientMessages.DBInfo[]{new ClientMessages.DBInfo("select * from %s ", "id", new String[]{"import_cps_confirm_1"}, "test", "localhost", "3306", "root", "lkmlnfqp")}
                , 4
                , "/Users/hesiyuan/honey-data/");
        HoneyClient.submitJob(job);
    }

    @Test
    public void test2() {

    }
}
