package data.sync.core.fetcher;

import data.sync.common.*;
import data.sync.core.WorkerStatistic;
import data.sync.core.storage.Line;
import data.sync.core.storage.Storage;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by hesiyuan on 15/6/29.
 */
public class MysqlFetcher implements Fetcher{
    private static Logger logger = Logger.getLogger(MysqlFetcher.class);
    Configuration conf = new Configuration();
    private volatile boolean stop = false;
    private String dbname;
    private String user;
    private String pwd;
    private String sql;
    private String table;
    private String ip;
    private String port;
    private String encode ="utf8";

    @Override
    public void init(ClusterMessages.TaskAttemptInfo attempt) {
        this.dbname = attempt.taskDesc().db();
        this.ip = attempt.taskDesc().ip();
        this.port = attempt.taskDesc().port();
        this.table = attempt.taskDesc().table();
        this.user = attempt.taskDesc().user();
        this.pwd = attempt.taskDesc().pwd();
        this.sql = attempt.taskDesc().sql();
        conf.addResource(Constants.CONFIGFILE_NAME);
    }

    @Override
    public void fetchDataToStorage(Storage storage, WorkerStatistic stat) throws Exception{
        DBSource.register(this.getClass(), ip, port, dbname, createProperties());
        Connection conn = DBSource.getConnection(this.getClass(), ip, port, dbname);
        try {
            long start = System.currentTimeMillis();
            ResultSet rs = DBUtils.query(conn, sql);
            int size = 0;
            if(rs.next()) {

                size = rs.getMetaData().getColumnCount();
                Line l = storage.createLine();
                for(int i=1;i<=size;i++)
                    l.addField(rs.getString(i));
                storage.push(l);
                stat.incReadNum(1);
            }

            while(rs.next()&&!stop){
                Line l = storage.createLine();
                for(int i=1;i<=size;i++)
                    l.addField(rs.getString(i));
                storage.push(l);
                stat.incReadNum(1);
            }
            logger.info("fetcher cost:"+(System.currentTimeMillis()-start));
        } finally{
            try {
                conn.close();
            } catch (SQLException e) {
            }
        }

    }

    @Override
    public void stop() {
            stop = true;
    }
    private Properties createProperties() {
        Properties p = new Properties();

        String encodeDetail = "";

        if(!StringUtils.isBlank(this.encode)){
            encodeDetail = "useUnicode=true&characterEncoding="	+ this.encode + "&";
        }
        String url = "jdbc:mysql://" + this.ip + ":" + this.port + "/"
                + this.dbname + "?" + encodeDetail
                + "yearIsDateType=false&zeroDateTimeBehavior=convertToNull&useCompression=true"
                + "&defaultFetchSize="+Integer.MIN_VALUE;

        p.setProperty("driverClassName", "com.mysql.jdbc.Driver");
        p.setProperty("url", url);
        p.setProperty("username", user);
        p.setProperty("password", pwd);
        p.setProperty("maxActive", conf.get("mysql.coon.max.active","200"));
        p.setProperty("initialSize", "10");
        p.setProperty("maxIdle", "10");
        p.setProperty("maxWait", "1000");
        p.setProperty("defaultReadOnly", "true");
        p.setProperty("testOnBorrow", "true");
        p.setProperty("validationQuery", "select 1 from dual");
        return p;
    }
}
