//package data.sync.supervisor.alarm.alarmimpl;
//
//import java.net.URLEncoder;
//import java.util.HashMap;
//import java.util.Map;
//
//import data.sync.supervisor.alarm.AbstractAlarm;
//import data.sync.supervisor.alarm.AlarmMessage;
//import data.sync.supervisor.main.Supervisor;
//import org.apache.commons.httpclient.HttpClient;
//import org.apache.commons.httpclient.HttpMethodBase;
//import org.apache.commons.httpclient.NameValuePair;
//import org.apache.commons.httpclient.methods.DeleteMethod;
//import org.apache.commons.httpclient.methods.GetMethod;
//import org.apache.commons.httpclient.methods.PostMethod;
//import org.apache.commons.httpclient.methods.PutMethod;
//import org.apache.commons.lang.StringUtils;
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//
//public class FetionAlarm extends AbstractAlarm {
//	private static final Log LOG = LogFactory.getLog(FetionAlarm.class);
//
//	public static enum HttpType {
//		POST, GET, PUT, DELETE
//	}
//
//	private HttpClient client;
//	private HttpType type;
//	private String params;
//	private String url;
//
//	public boolean init() {
//		type = HttpType.valueOf(Supervisor.conf
//				.get("supervisor.alarm.fetion.type", "GET").trim()
//				.toUpperCase());
//		params = Supervisor.conf.get("supervisor.alarm.fetion.params", "");
//		url = Supervisor.conf.get("supervisor.alarm.fetion.url", "");
//		client = new HttpClient();
//		client.getParams().setContentCharset("utf-8");
//		return true;
//	}
//
//	public boolean alarm(AlarmMessage message) {
//		try {
//			String[] targets = message.getTargets();
//			for(int i=0;i<targets.length;i++){
////				StringBuilder sb = new StringBuilder();
////				for (int i = 0; i < targets.length; i++) {
////					sb.append(targets[i]);
////					sb.append(",");
////				}
//				String target = targets[i];
//				Map<String, String> map = new HashMap<String, String>();
//				map.put("targets", target);
//				map.put("message", "【"+message.getTitle()+"】："+message.getBody());
//				this.execute(url, params, map, type);
//			}
//			return true;
//		} catch (Exception e) {
//			LOG.error("fetion alarm error", e);
//			return false;
//		}
//	}
//
//	// 生成对应method
//	private HttpMethodBase getMethod(String url, String params,
//			Map<String, String> map, HttpType type) throws Exception {
//		HttpMethodBase method = null;
//		if (params != null)
//			params.trim();
//		switch (type) {
//		case GET: {
//			url = fillUrl(url, params, map);
//			method = new GetMethod(url);
//			break;
//		}
//		case PUT: {
//			url = fillUrl(url, params, map);
//			method = new PutMethod(url);
//			break;
//		}
//		case DELETE: {
//			url = fillUrl(url, params, map);
//			method = new DeleteMethod(url);
//			break;
//		}
//		case POST: {
//			method = new PostMethod(url);
//			if (!StringUtils.isEmpty(params)) {
//				String[] parama = params.split(";");
//				NameValuePair[] pairs = new NameValuePair[parama.length];
//				NameValuePair name = null;
//				for (int i = 0; i < parama.length; i++) {
//					String[] iparama = parama[i].split(":");
//					try {
//						name = new NameValuePair(iparama[0],
//								map.get(iparama[1]));
//						pairs[i] = name;
//					} catch (Exception e) {
//						LOG.error(parama[i] + ":参数解析错误!");
//						throw new RuntimeException(e);
//					}
//				}
//				((PostMethod) method).setRequestBody(pairs);
//			}
//		}
//		}
//		LOG.info(url);
//		method.getParams().setContentCharset("utf-8");
//		return method;
//	}
//
//	// url参数填充
//	private String fillUrl(String url, String params, Map<String, String> map)
//			throws Exception {
//		StringBuilder sb = new StringBuilder(url);
//		if (!StringUtils.isEmpty(params)) {
//			String[] parama = params.split(";");
//			sb.append('?');
//			for (int i = 0; i < parama.length; i++) {
//				String[] iparama = parama[i].split(":");
//				sb.append(iparama[0]);
//				sb.append('=');
//				if (map == null || map.get(iparama[1]) == null)
//					sb.append("");
//				else
//					sb.append(URLEncoder.encode(map.get(iparama[1]), "utf-8"));
//					//sb.append(map.get(iparama[1]));
//				sb.append('&');
//
//			}
//		}
//		return sb.toString();
//	}
//
//	public void execute(String url, String params,
//			Map<String, String> map, HttpType type) {
//		HttpMethodBase method = null;
//		try {
//			method = getMethod(url, params, map, type);
//			client.executeMethod(method);
//		} catch (Exception e) {
//			LOG.error("", e);
//			try{
//			LOG.info(method.getResponseBodyAsString());
//			}catch(Exception ie){
//				LOG.error("",e);
//			}
//			throw new RuntimeException(e);
//		} finally {
//			if (method != null)
//				method.releaseConnection();
//		}
//	}
//
//}
