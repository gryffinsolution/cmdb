package util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RDao {
	private static final Logger LOG = LogManager.getLogger(RDao.class);

	public static void printSQLException(SQLException e) {
		while (e != null) {
			LOG.error("\n----- SQLException -----");
			LOG.error("  SQL State:  " + e.getSQLState());
			LOG.error("  Error Code: " + e.getErrorCode());
			LOG.error("  Message:    " + e.getMessage());
			e = e.getNextException();
		}
	}

	public Connection getConnection(String dbURL, String user, String password) {
		LOG.info("DB_URL=" + dbURL);
		LOG.info("user=" + user);
		LOG.info("password=" + password);
		Connection con = null;
		try {
			if (dbURL.startsWith("jdbc:postgresql:")) {
				Class.forName("org.postgresql.Driver");
			} else if (dbURL.startsWith("jdbc:oracle:")) {
				Class.forName("oracle.jdbc.driver.OracleDriver");
			}
		} catch (ClassNotFoundException e) {
			LOG.error("DB Driver loading error!");
			e.printStackTrace();
		}
		try {
			con = DriverManager.getConnection(dbURL, user, password);
		} catch (SQLException e) {
			LOG.error("getConn Exception");
			e.printStackTrace();
		}
		return con;
	}

	public void disconnect(Connection conn) {
		try {
			conn.close();
		} catch (SQLException e) {
			LOG.error("disConn Exception)");
			printSQLException(e);
		}
	}

	public int getTotalHostNo(Connection con) {
		Statement stmt;
		int NoOfHost = 0;
		try {
			String sql = "SELECT MAX(HOST_NO) FROM HOST_INFOS ";
			stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				NoOfHost = rs.getInt(1);
				LOG.info("Total hosts=" + NoOfHost);
			}
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			printSQLException(e);
		}
		return NoOfHost;
	}

	public ArrayList<String> getHostsMT(Connection con, int seq, int Total,
			String agentVersion, String sql) {
		Statement stmt;
		ArrayList<String> hostList = new ArrayList<String>();
		try {
			int NoOfHost = getTotalHostNo(con);
			int sliceTerm = (int) Math.ceil(NoOfHost / (Total * 1.0));
			LOG.info("GAP=>" + sliceTerm);
			int sliceStart = 0;
			int sliceEnd = 0;
			sliceStart = sliceStart + sliceTerm * seq;
			sliceEnd = sliceStart + sliceTerm - 1;
			LOG.info(seq + 1 + ":" + sliceStart + "~" + sliceEnd);
			sql = sql + " AND HOST_NO > " + sliceStart + " AND HOST_NO < "
					+ sliceEnd;
			LOG.info(sql);

			stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				String host = rs.getString("HOSTNAME");
				LOG.info(seq + 1 + ":hostname=" + host);
				hostList.add(host);
			}
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			printSQLException(e);
		}
		return hostList;
	}

	public ArrayList<String> getHostsTest() {
		ArrayList<String> host = new ArrayList<String>();
		host.add("localhost.localdomain");
		return host;
	}

	public boolean insertInfos(Connection conFLOG, String host,
			boolean isDelayed, HashMap<String, String> kvBaseItems,
			HashMap<String, String> kvStrItems,
			HashMap<String, Float> kvFloatItems) {

		PreparedStatement pstHostFLOG = null;
		PreparedStatement pstCmdbFLOG = null;
		int intIsDelayed = 0;
		if (isDelayed == true) {
			intIsDelayed = 1;
			LOG.info(host + " is busy:" + isDelayed);
		}
		try {

			conFLOG.setAutoCommit(false);
			String sqlBaseUpdt = "UPDATE HOST_INFOS SET ";

			if (host == null || host.matches("null")) {
				return false;
			}

			for (String key : kvBaseItems.keySet()) {
				LOG.info("K:" + key + ",V:" + kvBaseItems.get(key));
				sqlBaseUpdt = sqlBaseUpdt + key + "='" + kvBaseItems.get(key)
						+ "',";
			}
			sqlBaseUpdt = sqlBaseUpdt
					+ " IS_MONITORING=1, CMDB_LAST_TIME=sysdate ";
			sqlBaseUpdt = sqlBaseUpdt + " WHERE HOSTNAME='" + host + "'";
			LOG.trace(sqlBaseUpdt);
			pstHostFLOG = conFLOG.prepareStatement(sqlBaseUpdt);
			pstHostFLOG.executeUpdate();
			conFLOG.commit();

			String sqlInfoUpdtPre = "MERGE INTO CMDB_INFOS CI USING DUAL D ON (CI.HOSTNAME='"
					+ host + "' ";
			String sqlInfoUpdtMid = ") WHEN MATCHED THEN UPDATE SET CI.LAST_UPDATED_TIME=SYSDATE,"
					+ " CI.UP_DAYS ="
					+ kvFloatItems.get("UP_DAYS")
					+ " WHEN NOT MATCHED THEN INSERT (HOSTNAME";
			String sqlInfoUpdtPst = ") VALUES ('" + host;

			for (String key : kvStrItems.keySet()) {
				sqlInfoUpdtPre = sqlInfoUpdtPre + " AND CI." + key + "='"
						+ kvStrItems.get(key) + "'";
				sqlInfoUpdtMid = sqlInfoUpdtMid + "," + key;
				sqlInfoUpdtPst = sqlInfoUpdtPst + "','" + kvStrItems.get(key);
			}
			sqlInfoUpdtPst = sqlInfoUpdtPst + "'";
			for (String key : kvFloatItems.keySet()) {
				LOG.info("K:" + key + ",V:" + kvFloatItems.get(key));
				if (!key.matches("UP_DAYS")) {
					sqlInfoUpdtPre = sqlInfoUpdtPre + " AND D." + key + "="
							+ kvFloatItems.get(key);
				}
				sqlInfoUpdtMid = sqlInfoUpdtMid + "," + key;
				sqlInfoUpdtPst = sqlInfoUpdtPst + "," + kvFloatItems.get(key);
			}
			sqlInfoUpdtMid = sqlInfoUpdtMid + ",LAST_UPDATED_TIME";
			sqlInfoUpdtPst = sqlInfoUpdtPst + ",sysdate)";

			String sqlUpdt = sqlInfoUpdtPre + " " + sqlInfoUpdtMid + " "
					+ sqlInfoUpdtPst;

			LOG.trace(sqlUpdt);
			pstCmdbFLOG = conFLOG.prepareStatement(sqlUpdt);
			pstCmdbFLOG.executeUpdate();
			conFLOG.commit();

		} catch (SQLException sqle) {
			printSQLException(sqle);
		} catch (ArrayIndexOutOfBoundsException e) {
			LOG.error(e);
		} finally {
			try {
				if (pstHostFLOG != null)
					pstHostFLOG.close();
				if (pstCmdbFLOG != null)
					pstCmdbFLOG.close();
			} catch (SQLException e) {
				printSQLException(e);
			}
			pstHostFLOG = null;
			pstCmdbFLOG = null;
		}
		return true;
	}

	public HashMap<String, Boolean> getV3Info(Connection conn) {
		HashMap<String, Boolean> isV3 = new HashMap<String, Boolean>();
		Statement stmt;
		try {
			String sql = "SELECT DISTINCT HOSTNAME,IS_V3 FROM HOST_INFOS WHERE IS_V3=1 ";
			LOG.info(sql);
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				String host = rs.getString("HOSTNAME");
				isV3.put(host, true);
			}
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			printSQLException(e);
		}
		return isV3;
	}

	public void checkLic(String licFile, Connection conCMDB, String sql,
			String reportFile) {

		int NodesPurchased = 0;
		int NodesEvaluation = 0;
		StringBuffer sb = new StringBuffer("F.L.O.G. license check report\n");
		try {
			File keyFile = new File(licFile);
			FileReader fileReader = new FileReader(keyFile);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String line;

			while ((line = bufferedReader.readLine()) != null) {
				if (line.startsWith("NodesPurchased")) {
					String[] items = line.split(":");
					if (items.length >= 2) {
						NodesPurchased = Integer.parseInt(items[1].trim());
						LOG.info("NodesPurchased:" + NodesPurchased);
						sb.append("NodesPurchased:" + NodesPurchased + "\n");
					}
				}
				if (line.startsWith("NodesEvaluation")) {
					String[] items = line.split(":");
					if (items.length >= 2) {
						NodesEvaluation = Integer.parseInt(items[1].trim());
						LOG.info("NodesEvaluation:" + NodesEvaluation);
						sb.append("NodesEvaluation:" + NodesEvaluation + "\n");
					}
				}
				if (line.startsWith("EvalBaseDate")) {
					String[] items = line.split(":");
					if (items.length >= 2) {
						LOG.info("EvaluationBaseDate:" + items[1]);
						sb.append("EvaluationBaseDate:" + items[1] + "\n");
					}
				}
			}
			bufferedReader.close();
			fileReader.close();
		} catch (IOException e) {
			LOG.fatal("there is no license file at " + licFile);
		}

		Statement stmt;
		String host = null;
		int i = 1;
		try {
			stmt = conCMDB.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			sb.append("\nNew node list\n");
			sb.append("Node number / total purchased node : node name\n");
			while (rs.next()) {
				host = rs.getString(1);
				sb.append(i + "/" + NodesPurchased + ":" + host + "\n");
				if (i > NodesPurchased) {
					LOG.fatal("Non-Licensed Server Name = No.(" + i + "/"
							+ NodesPurchased + "):" + host);
				}
				i++;
			}
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			printSQLException(e);
		}
		if (i > NodesPurchased) {
			LOG.fatal("License exceed.\nNumber of purchased node license is "
					+ NodesPurchased + "\nNumber of license exceed is "
					+ (i - NodesPurchased)
					+ "\nPlease contact help@flogsoft.com");
			sb.append("License exceed.\nNumber of purchased node license is "
					+ NodesPurchased + "\nNumber of license exceed is "
					+ (i - NodesPurchased) + "\n");
			sb.append("Your license is not enough for operating mode\n");
			sb.append("Please contact help@flogsoft.com\n");
		}

		BufferedWriter bw = null;
		FileWriter fw = null;
		try {
			fw = new FileWriter(reportFile);
			bw = new BufferedWriter(fw);
			bw.write(sb.toString());
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (bw != null)
					bw.close();
				if (fw != null)
					fw.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
	}
}