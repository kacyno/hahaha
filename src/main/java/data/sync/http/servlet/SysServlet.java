package data.sync.http.servlet;


import data.sync.common.ReflectionUtils;
import data.sync.common.StringUtils;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;

/**
 * 打印当前内存及线程信息，用于监控
 * Created by hesiyuan on 15/2/2.
 */
public class SysServlet extends HttpServlet {
    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        response.setContentType("text/plain; charset=UTF-8");
        PrintWriter out = response.getWriter();
        out.println(getMemReport());
        out.println("----------------------------------------------------------------------------------------------");
        ReflectionUtils.printThreadInfo(out, "");
        out.close();
    }
    private String getMemReport() {
        MemoryMXBean mem = ManagementFactory.getMemoryMXBean();
        MemoryUsage heap = mem.getHeapMemoryUsage();
        long totalMemory = heap.getUsed();
        long maxMemory = heap.getMax();
        long commitedMemory = heap.getCommitted();

        MemoryUsage nonHeap = mem.getNonHeapMemoryUsage();
        long totalNonHeap = nonHeap.getUsed();
        //long maxNonHeap = nonHeap.getMax();
        long commitedNonHeap = nonHeap.getCommitted();

        long used = (totalMemory * 100) / commitedMemory;
        //long usedNonHeap = (totalNonHeap * 100) / commitedNonHeap;

        String str = "Heap Memory used "
                + StringUtils.byteDesc(totalMemory) + " is " + " " + used
                + "% of Commited Heap Memory "
                + StringUtils.byteDesc(commitedMemory)
                + ". Max Heap Memory is " + StringUtils.byteDesc(maxMemory)
                + ". ";
//		str += "<div>Non Heap Memory used "
//				+ StringUtils.byteDesc(totalNonHeap) + " is" + " "
//				+ usedNonHeap + "% of " + " Commited Non Heap Memory "
//				+ StringUtils.byteDesc(commitedNonHeap)
//				+ ". Max Non Heap Memory is "
//				+ StringUtils.byteDesc(maxNonHeap) + ".</div>";
        return str;

    }
}
