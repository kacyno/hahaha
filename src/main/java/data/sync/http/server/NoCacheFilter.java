
package data.sync.http.server;

import javax.servlet.*;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class NoCacheFilter implements Filter {

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
  }

  @Override
  public void doFilter(ServletRequest req, ServletResponse res,
                       FilterChain chain)
    throws IOException, ServletException {
    HttpServletResponse httpRes = (HttpServletResponse) res;
    httpRes.setHeader("Cache-Control", "no-cache");
    long now = System.currentTimeMillis();
    httpRes.addDateHeader("Expires", now);
    httpRes.addDateHeader("Date", now);
    httpRes.addHeader("Pragma", "no-cache");
    httpRes.setHeader("Access-Control-Allow-Origin", "*");
    chain.doFilter(req, res);
  }

  @Override
  public void destroy() {
  }

}
