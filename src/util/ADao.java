package util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ADao {
	private static final Logger LOG = LogManager.getLogger(ADao.class);

	public static String printSQLException(SQLException e) {
		LOG.error("\n----- SQLException -----");
		LOG.error("  SQL State:  " + e.getSQLState());
		LOG.error("  Error Code: " + e.getErrorCode());
		LOG.error("  Message:    " + e.getMessage());
		if (e.getMessage().contains("Table/View")
				|| e.getMessage().contains(" does not exist.")) {
			LOG.fatal(e.getMessage());
			return "error:NoTable";
		}
		if (e.getMessage().contains("Error connecting to server")) {
			LOG.info(e.getMessage());
			return "error:connection";
		}
		return "error:unknown";
	}

	public void getHostInfos(int port, String hostName,
			ArrayList<String> columns, HashMap<String, String> columnMap,
			HashMap<String, String> kvBaseItems,
			HashMap<String, String> kvStrItems,
			HashMap<String, Float> kvFloatItems) {

		Connection conn = null;
		Properties props = new Properties();
		props.put("user", "agent");
		props.put("password", "catallena7");
		String protocol = null;
		protocol = "jdbc:derby://" + hostName + ":" + port + "/";
		PreparedStatement pst = null;
		ResultSet rs = null;
		Statement s = null;

		try {
			DriverManager.setLoginTimeout(5);
			conn = DriverManager.getConnection(
					protocol + "derbyDB;create=true", props);
			String sql = "SELECT AGENT_VERSION,DISTRO_VERSION,KERNEL_VERSION,LAST_SENT_EVENT_ID,"
					+ "DATA_KEEPING_DAYS,AUTOGENT_VERSION FROM AGENT_MGR";
			LOG.info(hostName + ":" + sql);
			s = conn.createStatement();
			rs = s.executeQuery(sql);
			while (rs.next()) {
				kvBaseItems.put("AGENT_VERSION", rs.getString("AGENT_VERSION"));
				kvBaseItems.put("LAST_SENT_EVENT_ID",
						rs.getString("LAST_SENT_EVENT_ID"));
				kvBaseItems.put("DATA_KEEPING_DAYS",
						rs.getString("DATA_KEEPING_DAYS"));
				kvBaseItems.put("AUTOGENT_VERSION",
						rs.getString("AUTOGENT_VERSION"));
				kvBaseItems.put("KERNEL_VERSION",
						rs.getString("KERNEL_VERSION"));
				kvBaseItems.put("DISTRO_VERSION",
						rs.getString("DISTRO_VERSION"));
				break;
			}
			s.close();
			rs.close();
			sql = "SELECT TIME";
			for (String column : columns) {
				sql = sql + ", " + column;
			}
			sql = sql + " FROM HOST_INFO ORDER BY TIME DESC";

			LOG.trace(hostName + ":" + sql);
			s = conn.createStatement();
			rs = s.executeQuery(sql);
			while (rs.next()) {
				if (hostName == null || hostName.matches("null")) {
					LOG.info("HostName is null");
					break;
				}
				for (String column : columns) {

					if (columnMap.containsKey(column)) {
						String dataType = columnMap.get(column);
						if (dataType.startsWith("float")) {
							kvFloatItems.put(column, rs.getFloat(column));
						} else if (dataType.startsWith("varchar")) {
							kvStrItems.put(column, rs.getString(column));
						}
					} else {

					}
				}
				break;
			}
			s = conn.createStatement();
			rs = s.executeQuery(sql);

		} catch (SQLException sqle) {
			printSQLException(sqle);
		} finally {
			try {
				if (pst != null) {
					pst.close();
					pst = null;
				}
				if (s != null) {
					s.close();
					s = null;
				}
				if (rs != null) {
					rs.close();
					rs = null;
				}
				if (conn != null) {
					conn.close();
					conn = null;
				}
			} catch (SQLException e) {
				printSQLException(e);
			}
		}
		LOG.info(hostName + "done");
	}
}