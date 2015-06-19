package data.sync.common;

import org.apache.commons.logging.Log;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ReflectionUtils {
	static private ThreadMXBean threadBean = ManagementFactory
			.getThreadMXBean();
	private static long previousLogTime = 0;
	private static final Class<?>[] EMPTY_ARRAY = new Class[] {};

	private static final Map<Class<?>, Constructor<?>> CONSTRUCTOR_CACHE = new ConcurrentHashMap<Class<?>, Constructor<?>>();

	@SuppressWarnings("unchecked")
	public static <T> T newInstance(Class<T> theClass, Configuration conf) {
		T result;
		try {
			Constructor<T> meth = (Constructor<T>) CONSTRUCTOR_CACHE
					.get(theClass);
			if (meth == null) {
				meth = theClass.getDeclaredConstructor(EMPTY_ARRAY);
				meth.setAccessible(true);
				CONSTRUCTOR_CACHE.put(theClass, meth);
			}
			result = meth.newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return result;
	}

	public static void printThreadInfo(PrintWriter stream, String title) {
		final int STACK_DEPTH = 20;
		boolean contention = threadBean.isThreadContentionMonitoringEnabled();
		long[] threadIds = threadBean.getAllThreadIds();
		stream.println("Process Thread Dump: " + title);
		stream.println(threadIds.length + " active threads");
		for (long tid : threadIds) {
			ThreadInfo info = threadBean.getThreadInfo(tid, STACK_DEPTH);
			if (info == null) {
				stream.println("  Inactive");
				continue;
			}
			stream.println("Thread "
					+ getTaskName(info.getThreadId(), info.getThreadName())
					+ ":");
			Thread.State state = info.getThreadState();
			stream.println("  State: " + state);
			stream.println("  Blocked count: " + info.getBlockedCount());
			stream.println("  Waited count: " + info.getWaitedCount());
			if (contention) {
				stream.println("  Blocked time: " + info.getBlockedTime());
				stream.println("  Waited time: " + info.getWaitedTime());
			}
			if (state == Thread.State.WAITING) {
				stream.println("  Waiting on " + info.getLockName());
			} else if (state == Thread.State.BLOCKED) {
				stream.println("  Blocked on " + info.getLockName());
				stream.println("  Blocked by "
						+ getTaskName(info.getLockOwnerId(),
								info.getLockOwnerName()));
			}
			stream.println("  Stack:");
			for (StackTraceElement frame : info.getStackTrace()) {
				stream.println("    " + frame.toString());
			}
		}
		stream.flush();
	}

	private static String getTaskName(long id, String name) {
		if (name == null) {
			return Long.toString(id);
		}
		return id + " (" + name + ")";
	}

	public static void logThreadInfo(Log log, String title, long minInterval) {
		boolean dumpStack = false;
		if (log.isInfoEnabled()) {
			synchronized (ReflectionUtils.class) {
				long now = Time.now();
				if (now - previousLogTime >= minInterval * 1000) {
					previousLogTime = now;
					dumpStack = true;
				}
			}
			if (dumpStack) {
				ByteArrayOutputStream buffer = new ByteArrayOutputStream();
				printThreadInfo(new PrintWriter(buffer), title);
				log.info(buffer.toString());
			}
		}
	}
}
