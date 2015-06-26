
package data.sync.http.server;

import com.sun.jersey.spi.container.servlet.ServletContainer;
import data.sync.common.Configuration;
import data.sync.common.Constants;
import data.sync.http.servlet.SysServlet;
import data.sync.http.servlet.TailLogServlet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.FilterMapping;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.MultiException;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.webapp.WebAppContext;

import javax.servlet.http.HttpServlet;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.BindException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class HttpServer {
    public static final Log LOG = LogFactory.getLog(HttpServer.class);

    public static final String NO_CACHE_FILTER = "NoCacheFilter";
    private static final String APP_NAME = "honeycombx";
    protected final Server webServer;
    protected final Connector listener;
    protected final WebAppContext webAppContext;
    protected final Map<WebAppContext.Context, Boolean> defaultContexts =
            new HashMap<WebAppContext.Context, Boolean>();
    private final boolean listenerStartedExternally;

    public HttpServer(Configuration conf) throws IOException {
        this(APP_NAME, conf.get(Constants.HTTP_ADDR, Constants.HTTP_ADDR_DEFAULT), conf.getInt(Constants.HTTP_PORT, Constants.HTTP_PORT_DEFAULT), conf);
    }

    public HttpServer(String name, String bindAddress, int port,
                      Configuration conf) throws IOException {
        this(name, bindAddress, port, conf, null);
    }

    public HttpServer(String name, String bindAddress, int port,
                      Configuration conf, Connector connector) throws IOException {
        webServer = new Server();

        if (connector == null) {
            listenerStartedExternally = false;
            //创建listener并绑定地址与端口
            listener = createBaseListener(conf);
            if (bindAddress != null)
                listener.setHost(bindAddress);
            listener.setPort(port);
        } else {
            listenerStartedExternally = true;
            listener = connector;
        }

        webServer.addConnector(listener);

        int maxThreads = conf.getInt(Constants.HTTP_MAX_THREADS, Constants.HTTP_MAX_THREDS_DEFALUT);

        QueuedThreadPool threadPool = maxThreads == -1 ?
                new QueuedThreadPool() : new QueuedThreadPool(maxThreads);
        webServer.setThreadPool(threadPool);

        final String appDir = getWebAppsPath(name);
        ContextHandlerCollection contexts = new ContextHandlerCollection();
        webServer.setHandler(contexts);

        webAppContext = new WebAppContext();
        webAppContext.setDisplayName(name);
        webAppContext.setContextPath("/");
        webAppContext.setWar(appDir + "/" + name);
        addNoCacheFilter(webAppContext);
        webServer.setHandler(webAppContext);
        addDefaultServlets();
        //添加rest
        ServletHolder holder = new ServletHolder(ServletContainer.class);
        holder.setInitParameter("com.sun.jersey.config.property.resourceConfigClass", "com.sun.jersey.api.core.PackagesResourceConfig");
        holder.setInitParameter("com.sun.jersey.config.property.packages", "data.sync.http.restful");
        webAppContext.addServlet(holder, "/rest/*");

    }

    @SuppressWarnings("unchecked")
    private void addNoCacheFilter(WebAppContext ctxt) {
        defineFilter(ctxt, NO_CACHE_FILTER,
                NoCacheFilter.class.getName(), Collections.EMPTY_MAP, new String[]{"/*"});
    }

    public Connector createBaseListener(Configuration conf) throws IOException {
        return HttpServer.createDefaultChannelConnector();
    }

    public static Connector createDefaultChannelConnector() {
        SelectChannelConnector ret = new SelectChannelConnector();
        ret.setLowResourcesMaxIdleTime(10000);
        ret.setAcceptQueueSize(128);
        ret.setResolveNames(false);
        ret.setUseDirectBuffers(false);
        //ret.setHeaderBufferSize(1024 * 64);
        return ret;
    }


    protected void addDefaultServlets() {
        addServlet("log","/log", TailLogServlet.class);
        addServlet("sys","/sys",SysServlet.class);
    }

    public void addServlet(String name, String pathSpec,
                           Class<? extends HttpServlet> clazz) {
        addInternalServlet(name, pathSpec, clazz);
    }


    public void addInternalServlet(String name, String pathSpec,
                                   Class<? extends HttpServlet> clazz) {
        ServletHolder holder = new ServletHolder(clazz);
        if (name != null) {
            holder.setName(name);
        }
        webAppContext.addServlet(holder, pathSpec);
    }

    protected void defineFilter(WebAppContext ctx, String name,
                                String classname, Map<String, String> parameters, String[] urls) {

        FilterHolder holder = new FilterHolder();
        holder.setName(name);
        holder.setClassName(classname);
        holder.setInitParameters(parameters);
        FilterMapping fmap = new FilterMapping();
        fmap.setPathSpecs(urls);
//        fmap.setDispatches();
        fmap.setFilterName(name);
        ServletHandler handler = ctx.getServletHandler();
        handler.addFilter(holder, fmap);
    }


    protected String getWebAppsPath(String appName) throws FileNotFoundException {
        URL url = getClass().getClassLoader().getResource(appName);
        if (url == null)
            throw new FileNotFoundException(appName
                    + " not found in CLASSPATH");
        String urlString = url.toString();
        return urlString.substring(0, urlString.lastIndexOf('/'));
    }


    public void start() throws IOException {
        try {
            try {
                openListener();
                LOG.info("Jetty bound to port " + listener.getLocalPort());
                webServer.start();
            } catch (IOException ex) {
                LOG.info("HttpServer.start() threw a non Bind IOException", ex);
                throw ex;
            } catch (MultiException ex) {
                LOG.info("HttpServer.start() threw a MultiException", ex);
                throw ex;
            }
            Handler[] handlers = webServer.getHandlers();
            for (int i = 0; i < handlers.length; i++) {
                if (handlers[i].isFailed()) {
                    throw new IOException(
                            "Problem in starting http server. Server handlers failed");
                }
            }
            Throwable unavailableException = webAppContext.getUnavailableException();
            if (unavailableException != null) {
                webServer.stop();
                throw new IOException("Unable to initialize WebAppContext",
                        unavailableException);
            }
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Problem starting http server", e);
        }
    }

    void openListener() throws Exception {
        if (listener.getLocalPort() != -1) {
            return;
        }
        if (listenerStartedExternally) {
            throw new Exception("Expected webserver's listener to be started " +
                    "previously but wasn't");
        }
        try {
            listener.close();
            listener.open();
        } catch (BindException ex) {
            BindException be = new BindException(
                    "Port in use: " + listener.getHost() + ":" + listener.getPort());
            be.initCause(ex);
            throw be;

        }

    }


    /**
     * stop the server
     */
    public void stop() throws Exception {
        MultiException exception = null;
        try {
            listener.close();
        } catch (Exception e) {
            LOG.error("Error while stopping listener for webapp"
                    + webAppContext.getDisplayName(), e);
            exception = addMultiException(exception, e);
        }
        try {
            webAppContext.clearAttributes();
            webAppContext.stop();
        } catch (Exception e) {
            LOG.error("Error while stopping web app context for webapp "
                    + webAppContext.getDisplayName(), e);
            exception = addMultiException(exception, e);
        }
        try {
            webServer.stop();
        } catch (Exception e) {
            LOG.error("Error while stopping web server for webapp "
                    + webAppContext.getDisplayName(), e);
            exception = addMultiException(exception, e);
        }

        if (exception != null) {
            exception.ifExceptionThrow();
        }

    }

    private MultiException addMultiException(MultiException exception, Exception e) {
        if (exception == null) {
            exception = new MultiException();
        }
        exception.add(e);
        return exception;
    }
}
