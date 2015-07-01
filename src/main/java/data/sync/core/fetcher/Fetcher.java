package data.sync.core.fetcher;

import data.sync.common.ClusterMessages;
import data.sync.core.WorkerStatistic;
import data.sync.core.storage.Storage;

/**
 * Created by hesiyuan on 15/6/29.
 */
public interface Fetcher {
    void init(ClusterMessages.TaskAttemptInfo attempt);
    void fetchDataToStorage(Storage storage,WorkerStatistic stat) throws Exception;
    void stop();
}
