package data.sync.core;

import java.io.Serializable;

/**
 * Created by hesiyuan on 15/6/24.
 */
public enum JobStatus {
    SUBMITED/*提交*/,STARTED/*放入调度器*/,RUNNING/*任务开始被执行*/,FINISHED/*正常结束*/,FAILED/*失败*/
}
