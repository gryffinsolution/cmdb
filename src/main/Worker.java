package main;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.Callable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import util.ADao;
import util.RDao;

public class Worker implements Callable<Boolean> {
	private static final Logger LOG = LogManager.getLogger(Worker.class);
	int thNo;
	int thAll;
	String rdbUrl;
	String rdbUser;
	String rdbPasswd;
	int agentPort;
	String agentVersion;
	int delayBaseSec;
	String sql;
	ArrayList<String> columns;
	HashMap<String, String> columnMap;

	public Worker(int thNo, int thAll, String rdbUrl, String rdbUser,
			String rdbPasswd, int agentPort, String agentVersion,
			int delayBaseSec, String sql, ArrayList<String> columns,
			HashMap<String, String> columnMap) {
		this.thNo = thNo;
		this.thAll = thAll;
		this.rdbUrl = rdbUrl;
		this.rdbUser = rdbUser;
		this.rdbPasswd = rdbPasswd;
		this.agentPort = agentPort;
		this.agentVersion = agentVersion;
		this.delayBaseSec = delayBaseSec;
		this.sql = sql;
		this.columns = columns;
		this.columnMap = columnMap;
	}

	@Override
	public Boolean call() throws Exception {
		RDao rDao = new RDao();
		Connection conFLOG = rDao.getConnection(rdbUrl, rdbUser, rdbPasswd);
		ArrayList<String> hosts = rDao.getHostsMT(conFLOG, thNo - 1, thAll,
				agentVersion, sql);
		// ArrayList<String> hosts = rDao.getHostsTest();
		ADao aDao = new ADao();
		int port = agentPort;
		int i = 0;

		for (String host : hosts) {
			i++;
			LOG.trace(thNo + "-" + i + ":" + host);
			HashMap<String, String> kvBaseItems = new HashMap<String, String>();
			HashMap<String, String> kvStrItems = new HashMap<String, String>();
			HashMap<String, Float> kvFloatItems = new HashMap<String, Float>();

			DateTime start = new DateTime();
			aDao.getHostInfos(port, host, columns, columnMap, kvBaseItems,
					kvStrItems, kvFloatItems);
			DateTime end = new DateTime();
			Duration elapsedTime = new Duration(start, end);
			long elapsedSecInt = (elapsedTime.getMillis() / 1000);
			boolean isDelayed = false;
			if (elapsedSecInt < delayBaseSec) {
				isDelayed = true;
			}
			LOG.fatal(host + " elapsed= " + elapsedTime + " " + elapsedSecInt);

			rDao.insertInfos(conFLOG, host, isDelayed, kvBaseItems, kvStrItems,
					kvFloatItems);
			kvBaseItems.clear();
			kvStrItems.clear();
			kvFloatItems.clear();
		}
		LOG.info(thNo + ",i=" + i);
		rDao.disconnect(conFLOG);
		return true;
	}
}