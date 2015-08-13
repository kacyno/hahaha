package data.sync.disruptor;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by hesiyuan on 15/7/6.
 */
public class SyncTest {
    public static void main(String[] args) {
        final long start = System.currentTimeMillis();
        List<Integer> list1 =new  LinkedList<Integer>();
        final LinkedBlockingQueue<Long> list2 = new LinkedBlockingQueue<Long>();
        final ArrayBlockingQueue<Long> list3 = new ArrayBlockingQueue<Long>(10000000);


        new Thread(){
            @Override
            public void run() {
                while(true) {
                    try {
                        if(list2.take()==99999999){
                            break;
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                System.out.println(System.currentTimeMillis()-start);
            }
        }.start();
        for(long i=0;i<100000000;i++){
            list2.add(i);
        }
    }
}
