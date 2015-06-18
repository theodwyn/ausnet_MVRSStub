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
	HashMap<String,String> ProductMap = new HashMap<String, String>();

	private PreparedStatement preparedStatement = null;

	private static RuntimeProperties properties = RuntimeProperties.getInstance();

	public EIP_DAO() {
		openConnection();

		sysdate = getSysdate();
		
		String Products = properties.getProperty("ddProduct");
		String[] Product = Products.split(";");
		for (int i=0; i<Product.length;i++) {
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

	public boolean insertBILL_REQUEST(Request req) {

		boolean result = false;

		for (String key : ProductMap.keySet()) {

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
		sb.append("  (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ");

		try {
			if (this.connection == null) {
				openConnection();
			}
			preparedStatement = connection.prepareStatement(sb.toString());
			
			preparedStatement.setString(1,"emapp.SEQ_BILLING_REQUEST.NEXTVAL");
			preparedStatement.setString(2,"READ FOUND");
			preparedStatement.setDate(3,formatSqlDate(0,0));
			preparedStatement.setString(4,"SP AUSNET");
			preparedStatement.setString(5,req.getSDP().substring(0,10));
			preparedStatement.setString(6,req.getSDP());
			preparedStatement.setString(7, req.getMeterRef());
			preparedStatement.setString(8,req.getSDP());
			preparedStatement.setDate(9,formatSqlDate(0,0));
			preparedStatement.setDate(10,formatSqlDate(-2,14*60));
			preparedStatement.setDate(11,formatSqlDate(-1,14*60));
			preparedStatement.setString(12,"GMT+10:00");
			preparedStatement.setString(13,"P");
			preparedStatement.setDate(14,formatSqlDate(-1,0));
			preparedStatement.setInt(15,2);
			preparedStatement.setString(16,"SPA,BEA,NEM12,LR,EASTENGY");
			preparedStatement.setString(17,"LOADED");
			preparedStatement.setDate(18,formatSqlDate(2,0));
			preparedStatement.setDate(19,formatSqlDate(-1,0));
			preparedStatement.setDate(20,formatSqlDate(2,0));
			preparedStatement.setString(21,ProductMap.get(key));
			preparedStatement.setString(22,key);
			preparedStatement.setDate(23,formatSqlDate(0,-10*60));
			preparedStatement.setInt(24,0);
			preparedStatement.setString(25,"S");
			preparedStatement.setString(26,"EXPORT_SENT");
			preparedStatement.setDate(27,formatSqlDate(-0,0));
			preparedStatement.setString(28,"BillingHistoryLoader");
			preparedStatement.setString(29,"SVT Generated");

			preparedStatement.addBatch();

			result = true;
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DbUtil.close(preparedStatement);
		}
		}
		return result;
	}

	public void executeBatch() {
		try {
			preparedStatement.executeBatch();
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