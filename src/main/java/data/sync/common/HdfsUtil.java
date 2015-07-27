package data.sync.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;

import java.io.IOException;


/**
 * Created by hesiyuan on 15/7/9.
 * 当使用公司版本hadoop时,scala编译不通过(原因尚未查出)，故做此工具类
 */
public class HdfsUtil {
    private FileSystem hdfs;
    private HdfsUtil(FileSystem hdfs){
        this.hdfs = hdfs;
    }
    public static HdfsUtil getHdfsUtil() throws IOException {
        return new HdfsUtil(FileSystem.get(new Configuration()));
    }
    public void close() throws IOException {
        hdfs.close();
    }
    public void rename(Path src,Path target) throws IOException {
        hdfs.rename(src,target);
    }
    public void delete(Path path) throws IOException {
        //集群限制，必须通过该方法才能删除
        new FsShell().delete(path,hdfs,true,true);
    }
    public void createNewFile(Path path) throws IOException {
        hdfs.createNewFile(path);
    }
}

