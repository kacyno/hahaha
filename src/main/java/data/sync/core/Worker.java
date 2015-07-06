package data.sync.core;

import data.sync.common.ClusterMessages;
import data.sync.common.Constants;
import data.sync.core.fetcher.Fetcher;
import data.sync.core.fetcher.MysqlFetcher;
import data.sync.core.sinker.HDFSSinker;
import data.sync.core.sinker.Sinker;
import data.sync.core.storage.RAMStorage;
import data.sync.core.storage.Storage;
import org.apache.log4j.Logger;
import java.io.IOException;

/**
 * Created by hesiyuan on 15/6/19.
 *
 *
 *
 * fetcher->storage 线程A
 * sinker->hdfs 线程B
 */
public class Worker {
    private static Logger logger = Logger.getLogger(Worker.class);
    private Bee bee;
    //任务的相关信息
    private ClusterMessages.TaskAttemptInfo attempt;
    private Storage storage;
    //从db中抽取数据，写入storage
    private FetcherExecutor fetcherE ;
    private Fetcher fetcher  = null;
    private SinkerExecutor sinkerE;
    //从storage读取数据，写入hdfs
    private Sinker sinker;
    //worker状态
    private String error;

    private WorkerStatistic stat;

    public String getError() {
        return error;
    }

    public long getReadCount() {
        return stat.getReadNum();
    }
    public long getBufferSize(){
        return storage.size();
    }
    public long getWriteCount() {
        return stat.getWriteNum();
    }


    public ClusterMessages.TaskAttemptInfo getAttempt() {
        return attempt;
    }
    public Worker(data.sync.common.Configuration conf,ClusterMessages.TaskAttemptInfo attempt,Bee bee) throws IOException {
        this.attempt = attempt;
        stat = new WorkerStatistic();
        this.bee = bee;
        fetcher = conf.getInstance(Constants.FETCHER_CLASSNAME,new MysqlFetcher());
        fetcherE = new FetcherExecutor();

        sinker = conf.getInstance(Constants.SINKER_CLASSNAME,new HDFSSinker());
        sinkerE = new SinkerExecutor();

        storage = new RAMStorage();
        storage.init(1000000,sinkerE,fetcherE);
    }
    public String getJobId(){
        return attempt.taskDesc().jobId();
    }

    public FetcherExecutor getFetcher(){
        return fetcherE;
    }
    public SinkerExecutor getSinker(){
        return sinkerE;
    }

    public class FetcherExecutor implements Runnable{
        private Thread current = null;
        @Override
        public void run() {
            try {
                current = Thread.currentThread();
                fetcher.init(attempt);
                fetcher.fetchDataToStorage(storage, stat);
            } catch (Exception e) {
                error = e.getMessage();
                bee.failTask(Worker.this);
                logger.info("fecher:"+attempt.toString(),e);
            }finally{
                storage.setPushClosed(true);
            }

        }
        public void stop(){
            current.interrupt();
            fetcher.stop();
        }
    }


    public  class SinkerExecutor implements Runnable{
        private Thread current = null;
        @Override
        public void run() {
            try {
                current = Thread.currentThread();
                sinker.init(attempt);
                sinker.sinkDataFromStorage(storage, stat);
                bee.successTask(Worker.this);
            } catch (Exception e) {
                error = e.getMessage();
                bee.failTask(Worker.this);
                logger.error("sinker:"+attempt.toString(),e);
            }
        }
        public void stop(){
            current.interrupt();
            sinker.stop();
        }
    }


}
