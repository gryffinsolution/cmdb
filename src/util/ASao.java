package util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ASao {
	private static final Logger LOG = LogManager.getLogger(ASao.class);

	public void getHostInfos(int port, String host, ArrayList<String> columns,
			HashMap<String, String> columnMap,
			HashMap<String, String> kvBaseItems,
			HashMap<String, String> kvStrItems,
			HashMap<String, Float> kvFloatItems) {

		String url = "http://" + host + ":" + port + "/gethostdataagntmgr";
		LOG.info(url);
		HttpURLConnection con = null;
		try {
			URL myurl = new URL(url);
			con = (HttpURLConnection) myurl.openConnection();
			con.setRequestMethod("GET");
			con.setReadTimeout(1000);
			con.setConnectTimeout(1000);
			StringBuilder content;
			try (BufferedReader in = new BufferedReader(new InputStreamReader(
					con.getInputStream()))) {
				String line;
				content = new StringBuilder();
				while ((line = in.readLine()) != null) {
					content.append(line);
					content.append(System.lineSeparator());
				}
			}
			String genInfoLines = content.toString();

			String[] genItems = genInfoLines.trim().split(",");

			if (genItems.length >= 3) {
				kvBaseItems.put("AGENT_VERSION", genItems[0]);
				kvBaseItems.put("LAST_SENT_EVENT_ID", genItems[1]);
				kvBaseItems.put("DATA_KEEPING_DAYS", genItems[2]);
			} else {
				return; // TODO : no return result
			}

			String sql = "";
			for (String column : columns) {
				sql = sql + "," + column;
			}
			url = "http://" + host + ":" + port + "/gethostinfos";
			LOG.info(url);
			myurl = new URL(url);
			con = (HttpURLConnection) myurl.openConnection();
			con.setRequestMethod("GET");
			try (BufferedReader in = new BufferedReader(new InputStreamReader(
					con.getInputStream()))) {
				String line;
				content = new StringBuilder();
				while ((line = in.readLine()) != null) {
					content.append(line);
					content.append(System.lineSeparator());
				}
			}
			String hostInfoLines = content.toString();
			String[] items = hostInfoLines.toString().split(",,");
			for (String item : items) {
				String[] kv = item.split("::");
				if (kv.length < 2) {
					continue;
				}
				if (columnMap.containsKey(kv[0])) {
					String dataType = columnMap.get(kv[0]);
					if (dataType.startsWith("float")) {
						kvFloatItems.put(kv[0], Float.parseFloat(kv[1]));
					} else if (dataType.startsWith("varchar")) {
						kvStrItems.put(kv[0], kv[1]);
					}
				}
			}
			con.disconnect();

		} catch (MalformedURLException e) {
			LOG.error(e);
		} catch (ProtocolException e) {
			LOG.error(e);
		} catch (SocketTimeoutException e) {
			LOG.error(host + " " + e); // TODO
		} catch (IOException e) {
			LOG.error(e);
		} catch (Exception e) {
			LOG.error(e);
		} finally {
			con.disconnect();
		}
		LOG.info(host + "done");
	}
}
