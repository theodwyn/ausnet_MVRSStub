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

	private java.sql.Date UTC_BILL_DATE;
	private java.sql.Date UTC_BILL_DATE_MINUS1;

	HashMap<String, String> ProductMap = new HashMap<String, String>();

	private PreparedStatement BILLING_REQUEST_stmt = null;
	private PreparedStatement LAST_INTERVAL_SET_stmt = null;

	private static RuntimeProperties properties = RuntimeProperties.getInstance();

	public EIP_DAO() {
		openConnection();
		BILLING_REQUEST_stmt = prepareBILLING_REQUEST();
		LAST_INTERVAL_SET_stmt = prepareLAST_INTERVAL_SET();

		this.setBillDates();
		this.loadProducts();
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

	private PreparedStatement prepareLAST_INTERVAL_SET() {
		PreparedStatement stmt = null;
		StringBuilder sb = new StringBuilder();

		sb.append("UPDATE ").append(this.dbOwner).append(".LAST_INTERVAL_SET ");
		sb.append("  SET UTC_LAST_END_TIME = ? ");
		sb.append("  WHERE CHANNEL_ID = ? ");

		try {
			stmt = connection.prepareStatement(sb.toString());
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return stmt;
	}

	public boolean updateLAST_INTERVAL_SET(Request req) {

		boolean result = false;

		try {
			LAST_INTERVAL_SET_stmt.setDate(1, UTC_BILL_DATE);
			LAST_INTERVAL_SET_stmt.setString(2, req.getMeterRef());

			LAST_INTERVAL_SET_stmt.addBatch();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return result;
	}

	private PreparedStatement prepareBILLING_REQUEST() {

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
		sb.append("    SENT_FOR_BILLING_FLAG, ");
		sb.append("    EXPORT_STATUS, ");
		sb.append("    LAST_UPD_TIME, ");
		sb.append("    LAST_UPD_BY, ");
		sb.append("    COMMENTS ");
		sb.append("  ) ");
		sb.append("  VALUES ");
		sb.append("  (EMAPP.SEQ_BILLING_REQUEST.NEXTVAL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ");

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
				BILLING_REQUEST_stmt.setString(1, "READ FOUND");
				BILLING_REQUEST_stmt.setDate(2, formatSqlDate(0, 0));
				BILLING_REQUEST_stmt.setString(3, "SP AUSNET");
				BILLING_REQUEST_stmt.setString(4, req.getSDP().substring(0, 10));
				BILLING_REQUEST_stmt.setString(5, req.getSDP());
				BILLING_REQUEST_stmt.setString(6, req.getSDPRef());
				BILLING_REQUEST_stmt.setString(7, req.getSDP());
				BILLING_REQUEST_stmt.setDate(8, formatSqlDate(0, 0));
				BILLING_REQUEST_stmt.setDate(9, UTC_BILL_DATE_MINUS1);
				BILLING_REQUEST_stmt.setDate(10, UTC_BILL_DATE);
				BILLING_REQUEST_stmt.setString(11, "GMT+10:00");
				BILLING_REQUEST_stmt.setString(12, "P");
				BILLING_REQUEST_stmt.setDate(13, UTC_BILL_DATE);
				BILLING_REQUEST_stmt.setString(14, "2");
				BILLING_REQUEST_stmt.setString(15, "SPA,BEA,NEM12,LR,EASTENGY");
				BILLING_REQUEST_stmt.setString(16, "LOADED");
				BILLING_REQUEST_stmt.setDate(17, formatSqlDate(2, 0));
				BILLING_REQUEST_stmt.setDate(18, UTC_BILL_DATE);
				BILLING_REQUEST_stmt.setDate(19, formatSqlDate(2, 0));
				BILLING_REQUEST_stmt.setString(20, ProductMap.get(key));
				BILLING_REQUEST_stmt.setString(21, key);
				BILLING_REQUEST_stmt.setDate(22, formatSqlDate(0, -10 * 60));
				BILLING_REQUEST_stmt.setInt(23, 0);
				BILLING_REQUEST_stmt.setString(24, "S");
				BILLING_REQUEST_stmt.setString(25, "EXPORT_SENT");
				BILLING_REQUEST_stmt.setDate(26, formatSqlDate(0, 0));
				BILLING_REQUEST_stmt.setString(27, "BillingHistoryLoader");
				BILLING_REQUEST_stmt.setString(28, "SVT Generated");

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

	private void setBillDates() {
		Calendar cal = Calendar.getInstance();

		cal.set(Calendar.HOUR_OF_DAY, 14);
		cal.clear(Calendar.MINUTE);
		cal.clear(Calendar.SECOND);
		cal.clear(Calendar.MILLISECOND);

		cal.add(Calendar.DATE, -1);
		UTC_BILL_DATE = new java.sql.Date(cal.getTime().getTime());

		cal.add(Calendar.DATE, -1);
		UTC_BILL_DATE_MINUS1 = new java.sql.Date(cal.getTime().getTime());
	}

	private java.sql.Date formatSqlDate(int dayOffset, int minuteOffset) {
		Calendar cal = Calendar.getInstance();
		if (dayOffset != 0) {
			cal.add(Calendar.DATE, dayOffset);
		}

		if (minuteOffset != 0) {
			cal.add(Calendar.MINUTE, minuteOffset);
		}

		return new java.sql.Date(cal.getTime().getTime());
	}

	private void loadProducts() {
		String Products = properties.getProperty("ddProduct");
		String[] Product = Products.split(";");
		for (int i = 0; i < Product.length; i++) {
			String[] ProductParts = Product[i].split(",");
			ProductMap.put(ProductParts[0], ProductParts[1]);
		}
	}
}