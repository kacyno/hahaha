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
        long start = System.currentTimeMillis();
        List<Integer> list1 =new  LinkedList<Integer>();
        Queue<Integer> list2 = new LinkedBlockingQueue<Integer>();
        Queue<Integer> list3 = new ArrayBlockingQueue<Integer>(10000000);
        for(int i=0;i<1000000;i++){
            list3.add(i);
        }
        System.out.println(System.currentTimeMillis()-start);
    }
}
