package data.sync.core;

import data.sync.common.ClusterMessages;
import data.sync.common.DBSource;
import data.sync.common.DBUtils;
import data.sync.core.storage.Line;
import data.sync.core.storage.RAMStorage;
import data.sync.core.storage.Storage;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

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

    //任务的相关信息
    private ClusterMessages.TaskAttemptInfo attempt;
    private Storage storage;
    //从db中抽取数据，写入storage
    private Fetcher fetcher ;
    //从storage读取数据，写入hdfs
    private Sinker sinker;
    //bee引用，用来更新状态
    private Bee bee;
    //worker状态
    private String error;


    public String getError() {
        return error;
    }

    private long readCount  =0;
    public long getReadCount() {
        return readCount;
    }
    private long writeCount =0;

    public long getWriteCount() {
        return writeCount;
    }
    public ClusterMessages.TaskAttemptInfo getAttempt() {
        return attempt;
    }
    public Worker(ClusterMessages.TaskAttemptInfo attempt,Bee bee) throws IOException {
        this.attempt = attempt;
        storage = new RAMStorage();
        storage.init("1",100,100,100);
        fetcher = new Fetcher(attempt,storage,bee);
        sinker = new Sinker(attempt,bee,storage);
    }
    public String getJobId(){
        return attempt.taskDesc().jobId();
    }

    public Fetcher getFetcher(){
        return fetcher;
    }
    public Sinker getSinker(){
        return sinker;
    }

    public class Fetcher implements Runnable{
        private volatile boolean stop = false;
        private final String dbname;
        private final String user;
        private final String pwd;
        private final String sql;
        private final String table;
        private final String ip;
        private final String port;
        private final Bee bee;
        private final Storage storage;
        private final String encode ="utf8";


        public Fetcher(ClusterMessages.TaskAttemptInfo attempt,Storage storage,Bee bee){
            this.dbname = attempt.taskDesc().db();
            this.ip = attempt.taskDesc().ip();
            this.port = attempt.taskDesc().port();
            this.table = attempt.taskDesc().table();
            this.user = attempt.taskDesc().user();
            this.pwd = attempt.taskDesc().pwd();
            this.sql = attempt.taskDesc().sql();
            this.bee = bee;
            this.storage = storage;
        }
        private Properties createProperties() {
            Properties p = new Properties();

            String encodeDetail = "";

            if(!StringUtils.isBlank(this.encode)){
                encodeDetail = "useUnicode=true&characterEncoding="	+ this.encode + "&";
            }
            String url = "jdbc:mysql://" + this.ip + ":" + this.port + "/"
                    + this.dbname + "?" + encodeDetail
                    + "yearIsDateType=false&zeroDateTimeBehavior=convertToNull"
                    + "&defaultFetchSize=" + String.valueOf(Integer.MIN_VALUE);

            p.setProperty("driverClassName", "com.mysql.jdbc.Driver");
            p.setProperty("url", url);
            p.setProperty("username", user);
            p.setProperty("password", pwd);
            p.setProperty("maxActive", "10");
            p.setProperty("initialSize", "10");
            p.setProperty("maxIdle", "1");
            p.setProperty("maxWait", "1000");
            p.setProperty("defaultReadOnly", "true");
            p.setProperty("testOnBorrow", "true");
            p.setProperty("validationQuery", "select 1 from dual");
            return p;
        }
        @Override
        public void run() {
            DBSource.register(this.getClass(),ip,port,dbname,createProperties());
            Connection conn = DBSource.getConnection(this.getClass(), ip, port, dbname);
            try {
                long start = System.currentTimeMillis();
                ResultSet rs = DBUtils.query(conn,sql);
                int size = 0;
                if(rs.next()) {
                    size = rs.getMetaData().getColumnCount();
                    Line l = storage.createLine();
                    for(int i=1;i<=size;i++)
                        l.addField(rs.getString(i));
                    storage.push(l);
                }
                while(rs.next()&&!stop){
                    Line l = storage.createLine();
                    for(int i=1;i<=size;i++)
                        l.addField(rs.getString(i));
                    storage.push(l);
                    readCount++;
                }
                storage.setPushClosed(true);
                logger.info("fetcher cost:"+(System.currentTimeMillis()-start));
            } catch (SQLException e) {
                error = e.getMessage();
                bee.failTask(Worker.this);
                logger.info("fecher:"+attempt.toString(),e);
            }

        }
        public void stop(){
            stop = true;
        }
    }
    public  class Sinker implements Runnable{
        public static final char SEPARATOR = 0x01;
        private final FileSystem fs;
        private final Storage storage;
        private final Bee bee;
        private final Path target;
        private volatile boolean stop = false;


        public Sinker(ClusterMessages.TaskAttemptInfo attempt,Bee bee,Storage storage) throws IOException {
            Configuration conf = new Configuration();
            this.fs = FileSystem.get(conf);
            this.bee = bee;
            this.storage = storage;
            this.target = new Path(attempt.taskDesc().targetDir()+"/"+attempt.attemptId());
        }
        @Override
        public void run() {
            try {
                long start = System.currentTimeMillis();
                storage.sinkThread = Thread.currentThread();
                PrintWriter pw = new PrintWriter(new OutputStreamWriter(fs.create(target)));
                Line l = null;
                while((l = storage.pull())!=null&&!stop){
                    pw.println(l.toString(SEPARATOR));
                    writeCount++;
                }
                pw.close();
                logger.info("sinker cost:"+(System.currentTimeMillis()-start));
                bee.successTask(Worker.this);
            } catch (IOException e) {
                error = e.getMessage();
                bee.failTask(Worker.this);
                logger.info("sinker:"+attempt.toString(),e);
            }
        }
        public void stop(){
            stop = true;
            storage.sinkThread.interrupt();
        }
    }


}
