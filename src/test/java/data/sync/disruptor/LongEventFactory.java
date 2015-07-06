package data.sync.disruptor;

import com.lmax.disruptor.EventFactory;

/**
 * Created by hesiyuan on 15/7/6.
 */
public class LongEventFactory implements EventFactory<LongEvent>
{
    public LongEvent newInstance()
    {
        return new LongEvent();
    }
}