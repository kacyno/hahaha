package data.sync.core.sinker;

import data.sync.common.ClusterMessages;
import data.sync.core.WorkerStatistic;
import data.sync.core.storage.Line;
import data.sync.core.storage.Storage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.log4j.Logger;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

/**
 * Created by hesiyuan on 15/6/29.
 */
public class HDFSSinker implements Sinker {
    private static Logger logger = Logger.getLogger(HDFSSinker.class);

    public static final char SEPARATOR = '\t';
    private FileSystem fs;
    private Path target;
    private volatile boolean stop = false;
    private String tmpId;
    private String codec;
    @Override
    public void init(ClusterMessages.TaskAttemptInfo attempt) throws Exception{
        Configuration conf = new Configuration();
        tmpId = attempt.attemptId();
        this.fs = FileSystem.get(conf);
        this.target = new Path(attempt.taskDesc().targetDir()+"/"+attempt.attemptId());
        codec = attempt.taskDesc().codec();
    }

    @Override
    public void sinkDataFromStorage(Storage storage, WorkerStatistic stat) throws Exception{
        long start = System.currentTimeMillis();
        OutputStream os = fs.create(target);
        if("gz".equals(codec)) {
            GzipCodec codec = new GzipCodec();
            codec.setConf(new Configuration());
            os = codec.createOutputStream(os);
        }
        PrintWriter pw = new PrintWriter(new OutputStreamWriter(os));
        Line l = null;
        while((l = storage.pull())!=null&&!stop){
            pw.println(l.toString(SEPARATOR));
            stat.incWriteNum(1);
        }
        pw.close();
        logger.info(tmpId+" sinker cost:"+(System.currentTimeMillis()-start));
    }

    @Override
    public void stop() {
        stop  = true;
    }
}
