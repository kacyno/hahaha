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
        new HoneyClient().killJob(new ClientMessages.KillJob("job_30aee700-bfd2-45fc-81e6-7d367db0c219"));
    }

    @Test
    public void test2() {

    }
}
