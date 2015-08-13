package data.sync.core.storage;


import data.sync.core.Worker;

import java.text.SimpleDateFormat;
import java.util.Date;

public abstract class Storage {

	private int destructLimit = 0;

	private Worker.SinkerExecutor sinker = null;

    private Worker.FetcherExecutor fetcher = null;
    public Line createLine(){
        return new DefaultLine();
    }
    private int lineLimit = 0;
	public boolean init( int lineLimit,
			Worker.SinkerExecutor sinker,Worker.FetcherExecutor fetcher) {
		pushClosed = false;
		if (lineLimit <= 0 ) {
			return false;
		}
        this.lineLimit = lineLimit;
		this.sinker = sinker;
        this.fetcher = fetcher;
		return true;
	}
	
	abstract public boolean push(Line line);

	abstract public boolean push(Line[] lines);

	abstract public Line pull();

	abstract public int pull(Line[] lines);

	public boolean isPushClosed() {
		return pushClosed;
	}

	public void setPushClosed(boolean close) {
		pushClosed = close;
		return;
	}

	abstract public int size();

	abstract public boolean empty();

	public int getLineLimit(){
        return this.lineLimit;
    }

	private boolean pushClosed;


}
