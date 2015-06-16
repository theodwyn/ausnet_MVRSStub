package database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import spa.RuntimeProperties;

public class EIP_DAO {

	private Connection connection;
	private String dbOwner;

	private PreparedStatement preparedStatement = null;

	private static RuntimeProperties properties = RuntimeProperties.getInstance();

	public EIP_DAO() {
		openConnection();
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

	public boolean insertBILL_REQUEST(BILLING_REQUEST br) {

		boolean result = false;

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

		sb.append("  ) ");
		sb.append("  VALUES ");
		sb.append("  (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ");

		try {
			if (this.connection == null) {
				openConnection();
			}
			preparedStatement = connection.prepareStatement(sb.toString());
			preparedStatement.setString(1, br.getBILLING_REQUEST_ID());
			preparedStatement.setString(2, br.getREAD_STATUS());
			preparedStatement.setString(3, br.getUTC_EXCP_TRIGGER_TIME());
			preparedStatement.setString(4, br.getUTILITY_ID());
			preparedStatement.setString(5, br.getPREMISE_UDC_ID());
			preparedStatement.setString(6, br.getSDP_UDC_ID());
			preparedStatement.setString(7, br.getSDP_ROW_ID());
			preparedStatement.setString(8, br.getSDP_UNIVERSAL_ID());
			preparedStatement.setString(9, br.getINSERT_TIME());
			preparedStatement.setString(10, br.getUTC_REQUEST_START_TIME());
			preparedStatement.setString(11, br.getUTC_REQUEST_END_TIME());
			preparedStatement.setString(12, br.getPREMISE_TIMEZONE());
			preparedStatement.setString(13, br.getOFF_CYCLE_FLAG());
			preparedStatement.setString(14, br.getUTC_BILLING_END_TIME());
			preparedStatement.setString(15, br.getPROTOCOL_VERSION());
			preparedStatement.setString(16, br.getEXPORT_PROTOCOL());
			preparedStatement.setString(17, br.getLOADER_STATUS());
			preparedStatement.setString(18, br.getUTC_MAX_READ_WAIT_TIME());
			preparedStatement.setString(19, br.getUTC_MIN_END_TIME());
			preparedStatement.setString(20, br.getUTC_MAX_END_TIME());
			preparedStatement.setString(21, br.getDD_PRODUCT_ROW_ID());
			preparedStatement.setString(22, br.getDD_GROUP_NAME());
			preparedStatement.setString(23, br.getUTC_PROCESS_START_TIME());
			preparedStatement.setString(24, br.getOVERRIDE_IND());
			preparedStatement.setString(25, br.getSENT_FOR_BILLING());
			preparedStatement.setString(26, br.getEXPORT_STATUS());
			preparedStatement.setString(27, br.getLAST_UPD_TIME());
			preparedStatement.setString(28, br.getLAST_UPD_BY());

			preparedStatement.addBatch();

			result = true;
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DbUtil.close(preparedStatement);
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

}