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
        new HoneyClient().killJob(new ClientMessages.KillJob("job_f88c90f3-e4a8-4609-9be0-9659ef8bf484"));
    }

    @Test
    public void test2() {

    }
}
