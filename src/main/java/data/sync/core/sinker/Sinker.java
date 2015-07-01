package data.sync.core.sinker;

import data.sync.common.ClusterMessages;
import data.sync.core.WorkerStatistic;
import data.sync.core.storage.Storage;

/**
 * Created by hesiyuan on 15/6/29.
 */
public interface Sinker {
    void init(ClusterMessages.TaskAttemptInfo attempt) throws Exception;
    void sinkDataFromStorage(Storage storage,WorkerStatistic stat) throws Exception;
    void stop();
}
