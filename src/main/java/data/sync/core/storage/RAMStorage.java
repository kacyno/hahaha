/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 * 
 */


package data.sync.core.storage;


import org.apache.log4j.Logger;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;



public class RAMStorage extends Storage {
	private static final Logger log = Logger.getLogger(RAMStorage.class);

	//private DoubleQueue mars = null;
	private LinkedBlockingQueue<Line> mars = null;
	private final int waitTime = 3000; 

	public boolean init(String id, int lineLimit, int byteLimit,
			int destructLimit) {
		if (super.init(id, lineLimit, byteLimit, destructLimit) == false){
			return false;
		}
		this.mars = new LinkedBlockingQueue<Line>(500000);
		return true;
	}

	

	@Override
	public boolean push(Line line) {
		if (isPushClosed())
			return false;
		try {
			while (mars.offer(line, waitTime, TimeUnit.MILLISECONDS) == false) {
				getStat().incLineRRefused(1);
			}
		} catch (InterruptedException e) {
			return false;
		}
		getStat().incLineRx(1);
		getStat().incByteRx(line.length());
		return true;
	}



	@Override
	public boolean push(Line[] lines, int size) {
//		if (isPushClosed()) {
//			return false;
//		}
//
//		try {
//			while (mars.offer(lines, size, waitTime, TimeUnit.MILLISECONDS) == false) {
//
//				getStat().incLineRRefused(1);
//
//                /*
//                 * If destruct limit value setted and line refused more than desctruct limit value,
//                 * then close the Storage.
//                 * shenggong.wang@aliyun-inc.com
//                 * Oct 10, 2011
//                 */
//                if (getDestructLimit() > 0 && getStat().getLineRRefused() >= getDestructLimit()){
//                	if (!isPushClosed()){
//                		log.warn("Close RAMStorage for " + getStat().getId() + ". Queue:" + info() + " Timeout times:" + getStat().getLineRRefused());
//                        setPushClosed(true);
//                	}
//                    return false;
//                }
//
//			}
//		} catch (InterruptedException e) {
//			return false;
//		}
//
//		getStat().incLineRx(size);
//		for (int i = 0; i < size; i++) {
//			getStat().incByteRx(lines[i].length());
//		}

		return true;
	}

	@Override
	public boolean fakePush(int lineLength) {
		getStat().incLineRx(1);
		getStat().incByteRx(lineLength);
		return false;
	}


	@Override
	public Line pull() {
		Line line = null;
		try {
			if(isPushClosed()){
				line = mars.poll(1,TimeUnit.SECONDS);
			}else{
				line = mars.take();
			}
		} catch (InterruptedException e) {
            log.info("push is closed,no more element to take!");
			return null;
		}
		if (line != null) {
			getStat().incLineTx(1);
			getStat().incByteTx(line.length());
		}
		return line;
	}

	@Override
	public int pull(Line[] lines) {
//		int readNum = 0;
//		try {
//			while ((readNum = mars.poll(lines, waitTime, TimeUnit.MILLISECONDS)) == 0) {
//				getStat().incLineTRefused(1);
//			}
//		} catch (InterruptedException e) {
//			return 0;
//		}
//		if (readNum > 0) {
//			getStat().incLineTx(readNum);
//			for (int i = 0; i < readNum; i++) {
//				getStat().incByteTx(lines[i].length());
//			}
//		}
//		if (readNum == -1) {
//			return 0;
//		}
		return 0;
	}


	@Override
	public int size() {
		return mars.size();
	}

	@Override
	public boolean empty() {
		return (size() <= 0);
	}

	@Override
	public int getLineLimit() {
		return 0;
	}

	@Override
	public String info() {
		return "";
	}
	@Override
	public void setPushClosed(boolean close) {
		super.setPushClosed(close);
        //极端情况下sinkThread还没启动fetch就结束了，但这种情况下setPushClosed在pull之前被调用，并不会引起相关错误
        if(sinkThread!=null)
            sinkThread.interrupt();
//		mars.close();
	}

}
