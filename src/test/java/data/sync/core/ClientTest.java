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
        new HoneyClient().killJob(new ClientMessages.KillJob("job_b29b796b-72ae-498d-9fe3-f65f433999bc"));
    }

    @Test
    public void test2() {

    }
}
