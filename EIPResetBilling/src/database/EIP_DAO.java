package database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.HashMap;

import spa.Request;
import spa.RuntimeProperties;

public class EIP_DAO {

	private Connection connection;
	private String dbOwner;
	private Calendar sysdate;
	HashMap<String, String> ProductMap = new HashMap<String, String>();

	private PreparedStatement BILLING_REQUEST_stmt = null;

	private static RuntimeProperties properties = RuntimeProperties.getInstance();

	public EIP_DAO() {
		openConnection();
		BILLING_REQUEST_stmt = prepareBILLING_REQUEST();

		sysdate = getSysdate();

		String Products = properties.getProperty("ddProduct");
		String[] Product = Products.split(";");
		for (int i = 0; i < Product.length; i++) {
			String[] ProductParts = Product[i].split(",");
			ProductMap.put(ProductParts[0], ProductParts[1]);
		}
	}

	public void openConnection() {
		ConnectionFactory cf = new ConnectionFactory();
		connection = cf.getConnection("EIP");
		try {
			connection.setAutoCommit(false);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		this.dbOwner = properties.getProperty("EIPDBOWNER");
	}

	public void closeConnection() {
		DbUtil.close(connection);
	}

	public void commitConnection() {
		try {
			connection.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public PreparedStatement prepareBILLING_REQUEST() {

		PreparedStatement stmt = null;
		StringBuilder sb = new StringBuilder();

		sb.append("INSERT ");
		sb.append("INTO ").append(this.dbOwner).append(".BILLING_REQUEST ");
		sb.append("  ( ");
		sb.append("    BILLING_REQUEST_ID, ");
		sb.append("    READ_STATUS, ");
		sb.append("    UTC_EXCP_TRIGGER_TIME, ");
		sb.append("    UTILITY_ID, ");
		sb.append("    PREMISE_UDC_ID, ");
		sb.append("    SDP_UDC_ID, ");
		sb.append("    SDP_ROW_ID, ");
		sb.append("    SDP_UNIVERSAL_ID, ");
		sb.append("    INSERT_TIME, ");
		sb.append("    UTC_REQUEST_START_TIME, ");
		sb.append("    UTC_REQUEST_END_TIME, ");
		sb.append("    PREMISE_TIMEZONE, ");
		sb.append("    OFF_CYCLE_FLAG, ");
		sb.append("    UTC_BILLING_END_TIME, ");
		sb.append("    PROTOCOL_VERSION, ");
		sb.append("    EXPORT_PROTOCOL, ");
		sb.append("    LOADER_STATUS, ");
		sb.append("    UTC_MAX_READ_WAIT_TIME, ");
		sb.append("    UTC_MIN_END_TIME, ");
		sb.append("    UTC_MAX_END_TIME, ");
		sb.append("    DD_PRODUCT_ROW_ID, ");
		sb.append("    DD_GROUP_NAME, ");
		sb.append("    UTC_PROCESS_START_TIME, ");
		sb.append("    OVERRIDE_IND, ");
		sb.append("    SENT_FOR_BILLING, ");
		sb.append("    EXPORT_STATUS, ");
		sb.append("    LAST_UPD_TIME, ");
		sb.append("    LAST_UPD_BY, ");
		sb.append("    COMMENTS ");
		sb.append("  ) ");
		sb.append("  VALUES ");
		sb.append("  (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?); ");

		try {
			stmt = connection.prepareStatement(sb.toString());
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return stmt;
	}

	public boolean insertBILL_REQUEST(Request req) {

		boolean result = false;

		for (String key : ProductMap.keySet()) {

			try {
				BILLING_REQUEST_stmt.setString(1, "emapp.SEQ_BILLING_REQUEST.NEXTVAL");
				BILLING_REQUEST_stmt.setString(2, "READ FOUND");
				BILLING_REQUEST_stmt.setDate(3, formatSqlDate(0, 0));
				BILLING_REQUEST_stmt.setString(4, "SP AUSNET");
				BILLING_REQUEST_stmt.setString(5, req.getSDP().substring(0, 10));
				BILLING_REQUEST_stmt.setString(6, req.getSDP());
				BILLING_REQUEST_stmt.setString(7, req.getMeterRef());
				BILLING_REQUEST_stmt.setString(8, req.getSDP());
				BILLING_REQUEST_stmt.setDate(9, formatSqlDate(0, 0));
				BILLING_REQUEST_stmt.setDate(10, formatSqlDate(-2, 14 * 60));
				BILLING_REQUEST_stmt.setDate(11, formatSqlDate(-1, 14 * 60));
				BILLING_REQUEST_stmt.setString(12, "GMT+10:00");
				BILLING_REQUEST_stmt.setString(13, "P");
				BILLING_REQUEST_stmt.setDate(14, formatSqlDate(-1, 0));
				BILLING_REQUEST_stmt.setInt(15, 2);
				BILLING_REQUEST_stmt.setString(16, "SPA,BEA,NEM12,LR,EASTENGY");
				BILLING_REQUEST_stmt.setString(17, "LOADED");
				BILLING_REQUEST_stmt.setDate(18, formatSqlDate(2, 0));
				BILLING_REQUEST_stmt.setDate(19, formatSqlDate(-1, 0));
				BILLING_REQUEST_stmt.setDate(20, formatSqlDate(2, 0));
				BILLING_REQUEST_stmt.setString(21, ProductMap.get(key));
				BILLING_REQUEST_stmt.setString(22, key);
				BILLING_REQUEST_stmt.setDate(23, formatSqlDate(0, -10 * 60));
				BILLING_REQUEST_stmt.setInt(24, 0);
				BILLING_REQUEST_stmt.setString(25, "S");
				BILLING_REQUEST_stmt.setString(26, "EXPORT_SENT");
				BILLING_REQUEST_stmt.setDate(27, formatSqlDate(-0, 0));
				BILLING_REQUEST_stmt.setString(28, "BillingHistoryLoader");
				BILLING_REQUEST_stmt.setString(29, "SVT Generated");

				BILLING_REQUEST_stmt.addBatch();
			} catch (SQLException e) {
				e.printStackTrace();
			}

		}
		return result;
	}

	public void executeBatch() {
		try {
			BILLING_REQUEST_stmt.executeBatch();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private java.sql.Date formatSqlDate(int dayOffset, int minuteOffset) {
		Calendar cal = (Calendar) sysdate.clone();
		if (dayOffset != 0) {
			cal.add(Calendar.DATE, dayOffset);
		}

		if (minuteOffset != 0) {
			cal.add(Calendar.MINUTE, minuteOffset);
		}
		return new java.sql.Date(cal.getTime().getTime());
	}

	private Calendar getSysdate() {
		Calendar sysdate = Calendar.getInstance();
		sysdate.set(Calendar.HOUR_OF_DAY, 0);
		sysdate.clear(Calendar.HOUR);
		sysdate.clear(Calendar.MINUTE);
		sysdate.clear(Calendar.SECOND);
		sysdate.clear(Calendar.MILLISECOND);
		return sysdate;
	}
}