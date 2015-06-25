package data.sync.core;

import org.junit.Test;

/**
 * Created by hesiyuan on 15/6/25.
 */
public class BeeManagerTest {
    @Test
    public void test1(){
        BeeManager.connDic().put("bee1",new BeeDesc(5,10,"bee1",null));
        BeeManager.connDic().put("bee2",new BeeDesc(4,10,"bee2",null));
        BeeManager.connDic().put("bee3",new BeeDesc(8,10,"bee3",null));
        System.out.println(BeeManager.getMostFreeBee());

    }
}
