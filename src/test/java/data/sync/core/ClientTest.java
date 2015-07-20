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
        new HoneyClient().killJob(new ClientMessages.KillJob("job_e592476d-8269-4719-8c12-3b7784ac897d"));
    }

    @Test
    public void test2() {

    }
}
