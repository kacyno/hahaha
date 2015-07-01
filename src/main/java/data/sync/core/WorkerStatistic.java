package data.sync.core;

/**
 * Created by hesiyuan on 15/6/29.
 */
public class WorkerStatistic {
    private long readNum = 0l;
    private long writeNum = 0l;

    public long getReadNum() {
        return readNum;
    }

    public void incReadNum(long inc) {
        this.readNum = readNum+inc;
    }

    public long getWriteNum() {
        return writeNum;
    }

    public void incWriteNum(long inc) {
        this.writeNum = writeNum+inc;
    }
}
