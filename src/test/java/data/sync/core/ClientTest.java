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
        new HoneyClient().killJob(new ClientMessages.KillJob("job_071af3c2-95c2-4fa7-b9d6-a74f4b4a750f"));
    }

    @Test
    public void test2() {

    }
}
