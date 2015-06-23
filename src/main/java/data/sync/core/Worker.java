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

    public static enum WorkerStatus{
        START,RUNNING,END
    }
    //任务的相关信息
    private ClusterMessages.TaskAttemptDesc attempt;
    private Storage storage;
    //从db中抽取数据，写入storage
    private Fetcher fetcher ;
    //从storage读取数据，写入hdfs
    private Sinker sinker;
    //bee引用，用来更新状态
    private Bee bee;
    //worker状态
    private volatile WorkerStatus status ;
    public Worker(ClusterMessages.TaskAttemptDesc attempt,Bee bee) throws IOException {
        this.attempt = attempt;
        storage = new RAMStorage();
        storage.init("1",100,100,100);
        fetcher = new Fetcher(attempt,storage,bee);
        sinker = new Sinker(this,attempt,bee,storage);
    }

    public WorkerStatus getStatus() {
        return status;
    }

    public Fetcher getFetcher(){
        return fetcher;
    }
    public Sinker getSinker(){
        return sinker;
    }

    public static class Fetcher implements Runnable{
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
        public Fetcher(ClusterMessages.TaskAttemptDesc attempt,Storage storage,Bee bee){
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
                while(rs.next()){
                    Line l = storage.createLine();
                    for(int i=1;i<=size;i++)
                        l.addField(rs.getString(i));
                    storage.push(l);
                }
                storage.setPushClosed(true);
                logger.info("fetcher cost:"+(System.currentTimeMillis()-start));
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }
    }
    public static class Sinker implements Runnable{
        public static final char SEPARATOR = 0x01;
        private final FileSystem fs;
        private final Storage storage;
        private final Bee bee;
        private final Path target;
        private final Worker worker;
        public Sinker(Worker worker,ClusterMessages.TaskAttemptDesc attempt,Bee bee,Storage storage) throws IOException {
            Configuration conf = new Configuration();
            this.fs = FileSystem.get(conf);
            this.bee = bee;
            this.storage = storage;
            this.worker = worker;
            this.target = new Path(attempt.taskDesc().targetDir()+"/"+attempt.attemptId());
        }
        @Override
        public void run() {
            try {
                long start = System.currentTimeMillis();
                storage.sinkThread = Thread.currentThread();
                PrintWriter pw = new PrintWriter(new OutputStreamWriter(fs.create(target)));
                Line l = null;
                while((l = storage.pull())!=null){
                    pw.println(l.toString(SEPARATOR));
                }
                pw.close();
                logger.info("sinker cost:"+(System.currentTimeMillis()-start));
                bee.finishTask(worker);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


}
