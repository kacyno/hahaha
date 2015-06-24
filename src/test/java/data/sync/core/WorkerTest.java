package data.sync.core;

import data.sync.common.ClusterMessages;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;

/**
 * Created by hesiyuan on 15/6/23.
 */
public class WorkerTest {
    @Test
    public void test1() throws InterruptedException, IOException {
//        long start = System.currentTimeMillis();
//        ClusterMessages.TaskInfo td = new ClusterMessages.TaskInfo("select * from import_cps_confirm_1","localhost","3306","root","lkmlnfqp","test","import_cps_confirm_1","/Users/hesiyuan/honey-data",1);
//        ClusterMessages.TaskAttemptInfo tad= new ClusterMessages.TaskAttemptInfo(td,"attempt-"+ UUID.randomUUID());
//        Worker worker = new Worker(tad,new Bee("",""));
////        Thread ft = new Thread(worker.getFetcher());
////        ft.join();
////        ft.start();
//        worker.getFetcher().run();
//        worker.getSinker().run();
//        System.out.println(System.currentTimeMillis()-start);
    }
}
