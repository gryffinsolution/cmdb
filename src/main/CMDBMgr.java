package main;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import util.Conf;
import util.RDao;

public class CMDBMgr {
	private static final Logger LOG = LogManager.getLogger(CMDBMgr.class);

	public static void printSQLException(SQLException e) {
		while (e != null) {
			LOG.error("\n----- SQLException -----");
			LOG.error("  SQL State:  " + e.getSQLState());
			LOG.error("  Error Code: " + e.getErrorCode());
			LOG.error("  Message:    " + e.getMessage());
			e = e.getNextException();
		}
	}

	public static void main(String[] args) {
		Conf cf = new Conf();
		if (args.length != 0 && args[0] != null) {
			cf.setConfFile(args[0]);
		} else {
			LOG.error("there is no config.xml");
			System.exit(0);
		}

		String rdbUrl = cf.getSingleString("main_db_url");
		String rdbUser = cf.getSingleString("main_user");
		String rdbPasswd = cf.getSingleString("main_password");

		String cmdbUrl = cf.getSingleString("cmdb_db_url");
		String cmdbUser = cf.getSingleString("cmdb_user");
		String cmdbPasswd = cf.getSingleString("cmdb_password");

		int thAll = cf.getSinglefValue("no_of_thread");
		int agentPort = cf.getSinglefValue("agent_port");
		String agentVersion = cf.getSingleString("agent_version");
		int delayBaseSec = cf.getSinglefValue("delay_base_sec");
		String sql = cf.getSingleString("get_host_sql");
		ArrayList<String> columns = cf.getColumns();
		HashMap<String, String> columnMap = new HashMap<String, String>();
		for (String column : columns) {
			cf.getColumnType(columnMap, column);
		}
		ExecutorService pool = Executors.newFixedThreadPool(thAll);
		Set<Future<Boolean>> set = new HashSet<Future<Boolean>>();
		for (int thNo = 1; thNo <= thAll; thNo++) {
			Callable callable = new Worker(thNo, thAll, rdbUrl, rdbUser,
					rdbPasswd, agentPort, agentVersion, delayBaseSec, sql,
					columns, columnMap);
			Future future = pool.submit(callable);
			set.add(future);
		}
		pool.shutdown();
		if (cf.getSingleString("lic_chk_result_file").length() > 2) {
			RDao rDao = new RDao();
			Connection conCMDB = rDao.getConnection(cmdbUrl, cmdbUser,
					cmdbPasswd);
			rDao.checkLic(cf.getSingleString("lic_key_file"), conCMDB,
					cf.getSingleString("lic_check_sql"),
					cf.getSingleString("lic_chk_result_file"));
			rDao.disconnect(conCMDB);
		}
	}
}