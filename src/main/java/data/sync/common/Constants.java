package data.sync.common;

/**
 * Created by hesiyuan on 15/1/30.
 */
public interface Constants {
    String CONFIGFILE_NAME = "honeycombx.xml";
    //http设置
    String HTTP_ADDR = "http.addr";
    String HTTP_ADDR_DEFAULT = "0.0.0.0";
    String HTTP_PORT = "http.port";
    int HTTP_PORT_DEFAULT = 43933;
    String HTTP_MAX_THREADS = "http.max.threads";
    int HTTP_MAX_THREDS_DEFALUT = 100;

    String QUEEN_NAME = "queen";
    String BEE_NAME = "bee";

    String QUEEN_PORT = "queen.port";
    int QUEEN_PORT_DEFAULT = 43935;

    String QUEEN_ADDR = "queen.addr";
    String QUEEN_ADDR_DEFAULT = "0.0.0.0";

    String BEE_PORT = "bee.port";
    int BEE_PORT_DEFAULT = 43933;

    String BEE_ADDR = "bee.addr";
    String BEE_ADDR_DEFAULT = "0.0.0.0";

    String BEE_WORKER_NUM = "bee.worker.num";
    int BEE_WORKER_NUM_DEFAULT = 10;

    int QUEEN_CHECKER_INTERVAL= 10*1000;
    int BEE_HEARTBEATER_INTERVAL = 2*1000;
    int TASK_TIMEOUT = 10*60*1000;

    String HISTORY_DIR = "history.dir";
    String HISTORY_DIR_DEFAULT = "./";

    String FETCHER_CLASSNAME = "fetcher.classname";

    String SINKER_CLASSNAME = "sinker.classname";

    String ZK_URL ="zookeeper.url";

    String QUEEN_ZK_WORK_DIR = "queen.zk.workDir";
    String QUEEN_ZK_WORK_DIR_DEFAULT = "/honey";

    String QUEEN_RECOVERY_DIR = "queen.recovery.dir";
    String QUEEN_RECOVERY_DIR_DEFAULT="./recovery";

    String QUEENS_HOSTPORT = "queens.hostport";

    String RECOVERY_MODE = "recovery.mode"; //NONE,ZOOKEEPER,FILESYSTEM,CUSTOM
    String RECOVERY_MODE_DEFAULT = "FILESYSTEM";

    String CUSTOM_RECOVERY_FACTORY="custom.recovery.factory";

}
