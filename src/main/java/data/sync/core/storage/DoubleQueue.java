package data.sync.core.storage;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class DoubleQueue extends AbstractQueue<Line> implements
		BlockingQueue<Line>, java.io.Serializable {
	private static final long serialVersionUID = 1L;
	
	private int lineLimit;
	
	private final Line[] itemsA;
	
	private final Line[] itemsB;

	private ReentrantLock readLock, writeLock;
	

	private Condition notFull;
	
	private Condition awake;

	private Line[] writeArray, readArray;
	
	private volatile int writeCount, readCount;
	
	private int writeArrayHP, writeArrayTP, readArrayHP, readArrayTP;
	
	private boolean closed = false;
	
	private int spillSize = 0;

	private long lineRx = 0;
	
	private long lineTx = 0;
	
	private long byteRx = 0;

	public String info() {
		return lineRx + ":" + lineTx;
	}


	public DoubleQueue(int lineLimitt) {
		if (lineLimit <= 0 ) {
			throw new IllegalArgumentException(
					"Queue initial capacity can't less than 0!");
		}
		this.lineLimit = lineLimit;
		itemsA = new Line[lineLimit];
		itemsB = new Line[lineLimit];

		readLock = new ReentrantLock();
		writeLock = new ReentrantLock();

		notFull = writeLock.newCondition();
		awake = writeLock.newCondition();

		readArray = itemsA;
		writeArray = itemsB;
		spillSize = lineLimit * 8 / 10;
	}

	public int getLineLimit() {
		return lineLimit;
	}

	public void setLineLimit(int capacity) {
		this.lineLimit = capacity;
	}

	private void insert(Line line) {
		writeArray[writeArrayTP] = line;
		++writeArrayTP;
		++writeCount;
		++lineRx;
		byteRx += line.length();
	}

	private void insert(Line[] lines, int size) {
		for (int i = 0; i < size; ++i) {
			writeArray[writeArrayTP] = lines[i];
			++writeArrayTP;
			++writeCount;
			++lineRx;
			byteRx += lines[i].length();
		}
	}

	private Line extract() {
		Line e = readArray[readArrayHP];
		readArray[readArrayHP] = null;
		++readArrayHP;
		--readCount;
		++lineTx;
		return e;
	}


	private int extract(Line[] ea) {
		int readsize = Math.min(ea.length, readCount);
		for (int i = 0; i < readsize; ++i) {
			ea[i] = readArray[readArrayHP];
			readArray[readArrayHP] = null;
			++readArrayHP;
			--readCount;
			++lineTx;
		}
		return readsize;
	}



	private long queueSwitch(long timeout, boolean isInfinite)
			throws InterruptedException {
		writeLock.lock();
		try {
			if (writeCount <= 0) {
				if (closed) {
					return -2;
				}
				try {
					if (isInfinite && timeout <= 0) {
						awake.await();
						return -1;
					} else {
						return awake.awaitNanos(timeout);
					}
				} catch (InterruptedException ie) {
					awake.signal();
					throw ie;
				}
			} else {
				Line[] tmpArray = readArray;
				readArray = writeArray;
				writeArray = tmpArray;

				readCount = writeCount;
				readArrayHP = 0;
				readArrayTP = writeArrayTP;

				writeCount = 0;
				writeArrayHP = readArrayHP;
				writeArrayTP = 0;

				notFull.signal();
				// logger.debug("Queue switch successfully!");
				return -1;
			}
		} finally {
			writeLock.unlock();
		}
	}

	
	public boolean offer(Line line, long timeout, TimeUnit unit)
			throws InterruptedException {
		if (line == null) {
			throw new NullPointerException();
		}
		long nanoTime = unit.toNanos(timeout);
		writeLock.lockInterruptibly();
		try {
			for (;;) {
				if (writeCount < writeArray.length) {
					insert(line);
					if (writeCount == 1) {
						awake.signal();
					}
					return true;
				}

				// Time out
				if (nanoTime <= 0) {
					return false;
				}
				// keep waiting
				try {
					nanoTime = notFull.awaitNanos(nanoTime);
				} catch (InterruptedException ie) {
					notFull.signal();
					throw ie;
				}
			}
		} finally {
			writeLock.unlock();
		}
	}

	public boolean offer(Line[] lines, int size, long timeout, TimeUnit unit)
			throws InterruptedException {
		if (lines == null) {
			throw new NullPointerException();
		}
		long nanoTime = unit.toNanos(timeout);
		writeLock.lockInterruptibly();
		try {
			for (;;) {
				if (writeCount + size <= writeArray.length) {
					insert(lines, size);
					if (writeCount >= spillSize) {
						awake.signalAll();
					}
					return true;
				}

				// Time out
				if (nanoTime <= 0) {
					return false;
				}
				// keep waiting
				try {
					nanoTime = notFull.awaitNanos(nanoTime);
				} catch (InterruptedException ie) {
					notFull.signal();
					throw ie;
				}
			}
		} finally {
			writeLock.unlock();
		}
	}

	public void close() {
		writeLock.lock();
		try {
			closed = true;
			awake.signalAll();
		} finally {
			writeLock.unlock();
		}
	}

	
	public Line poll(long timeout, TimeUnit unit) throws InterruptedException {
		long nanoTime = unit.toNanos(timeout);
		readLock.lockInterruptibly();

		try {
			for (;;) {
				if (readCount > 0) {
					return extract();
				}

				if (nanoTime <= 0) {
					return null;
				}
				nanoTime = queueSwitch(nanoTime, true);
			}
		} finally {
			readLock.unlock();
		}
	}
	public int poll(Line[] ea, long timeout, TimeUnit unit)
			throws InterruptedException {
		long nanoTime = unit.toNanos(timeout);
		readLock.lockInterruptibly();

		try {
			for (;;) {
				if (readCount > 0) {
					return extract(ea);
				}

				if (nanoTime == -2) {
					return -1;
				}

				if (nanoTime <= 0) {
					return 0;
				}
				nanoTime = queueSwitch(nanoTime, false);
			}
		} finally {
			readLock.unlock();
		}
	}

	public Iterator<Line> iterator() {
		return null;
	}

	@Override
	public int size() {
		return (writeCount + readCount);
	}

	@Override
	public int drainTo(Collection<? super Line> c) {
		return 0;
	}

	@Override
	public int drainTo(Collection<? super Line> c, int maxElements) {
		return 0;
	}

	@Override
	public boolean offer(Line line) {
		try {
			return offer(line, 20, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		return false;
	}

	@Override
	public void put(Line e) throws InterruptedException {
	}

	@Override
	public int remainingCapacity() {
		return 0;
	}

	@Override
	public Line take() throws InterruptedException {
		return null;
	}

	@Override
	public Line peek() {
		return null;
	}

	@Override
	public Line poll() {
		try {
			return poll(20, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}

}
