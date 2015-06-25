package data.sync.common;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

public class NetUtil {
	public static InetAddress[] getInetAddressFormHost(String hostName){
		try {
			return InetAddress.getAllByName(hostName);
		} catch (UnknownHostException e) {
			return new InetAddress[]{};
		}
	}
	public static InetSocketAddress[] getInetSocketAddressFromUrl(String url){
		String host = StringUtils.getHostFromUrl(url);
		int port = StringUtils.getPortFromUrl(url);
		
		InetAddress[] ias= getInetAddressFormHost(host);
		InetSocketAddress[] isas = new InetSocketAddress[ias.length];
		for(int i=0;i<ias.length;i++){
			isas[i] =new InetSocketAddress(ias[i],port);
		}
		return isas;
	}
	public static String getHostname() {
		try {return "" + InetAddress.getLocalHost().getHostName();}
		catch(UnknownHostException uhe) {return "" + uhe;}
	}

	public static void main(String[] args) {
		System.out.println(getHostname());
	}
}
