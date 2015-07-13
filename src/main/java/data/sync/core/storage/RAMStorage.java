

package data.sync.core.storage;


import data.sync.core.Worker;
import org.apache.log4j.Logger;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;



public class RAMStorage extends Storage {
	private static final Logger log = Logger.getLogger(RAMStorage.class);

	private BlockingQueue<Line> mars = null;

	@Override
	public boolean init( int lineLimit,Worker.SinkerExecutor sinker,Worker.FetcherExecutor fetcher) {
		if (super.init(lineLimit,sinker,fetcher) == false){
			return false;
		}
		this.mars = new ArrayBlockingQueue<Line>(lineLimit);
		return true;
	}

	

	@Override
	public boolean push(Line line) {
		if (isPushClosed())
			return false;
		try {
			mars.put(line);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return true;
	}

	@Override
	public boolean push(Line[] lines, int size) {
		throw new RuntimeException("Not Supported");
	}



	@Override
	public Line pull() {
		Line line = null;
        while(true) {
            try {
                line = mars.poll(1, TimeUnit.SECONDS);
                if (line == null && isPushClosed())
                    return mars.poll();
                else if (line != null)
                    return line;
            } catch (InterruptedException e) {
                return null;
            }
        }
	}

	@Override
	public int pull(Line[] lines) {
		return 0;
	}

	@Override
	public int size() {
		return mars.size();
	}

	@Override
	public boolean empty() {
		return mars.isEmpty();
	}


}
